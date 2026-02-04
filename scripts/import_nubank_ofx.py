from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _parse_ofx_sgml(text: str) -> List[Dict[str, str]]:
    """
    Parser simples para OFX (formato SGML-like).
    Extrai blocos <STMTTRN>...</STMTTRN> e retorna lista de dicts.
    """
    blocks = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", text, flags=re.S | re.I)
    rows: List[Dict[str, str]] = []

    for b in blocks:
        def tag(name: str) -> str:
            m = re.search(rf"<{name}>([^\r\n<]+)", b, flags=re.I)
            return m.group(1).strip() if m else ""

        dtposted = tag("DTPOSTED")   # 20260107120000[-03:...]
        trnamt = tag("TRNAMT")       # -12.34
        memo = tag("MEMO") or tag("NAME")
        fitid = tag("FITID")

        rows.append({
            "DTPOSTED": dtposted,
            "TRNAMT": trnamt,
            "MEMO": memo,
            "FITID": fitid,
        })

    return rows


def _coerce_date(dtposted: str) -> pd.Timestamp:
    """
    Converte DTPOSTED (yyyymmdd...) para Timestamp.
    """
    m = re.match(r"(\d{4})(\d{2})(\d{2})", dtposted or "")
    if not m:
        return pd.NaT
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return pd.to_datetime(f"{y}-{mo}-{d}", errors="coerce")


def _coerce_amount(x: str) -> float:
    """
    Converte TRNAMT para float (padrão OFX usa ponto).
    """
    try:
        return float(str(x).strip())
    except Exception:
        return float("nan")


def import_nubank_ofx(ofx_path: Path) -> pd.DataFrame:
    """
    Lê OFX do Nubank e devolve DataFrame padronizado:
    date, description, amount, source, external_id
    """
    text = ofx_path.read_text(encoding="latin-1", errors="ignore")
    rows = _parse_ofx_sgml(text)
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Nenhuma transação encontrada no OFX (sem blocos <STMTTRN>).")

    out = pd.DataFrame({
        "date": df["DTPOSTED"].map(_coerce_date),
        "description": df["MEMO"].astype(str).str.strip(),
        "amount": df["TRNAMT"].map(_coerce_amount),
        "source": "nubank",
        "external_id": df["FITID"].astype(str).str.strip(),
        "file_name": ofx_path.name,
    })

    out = out.dropna(subset=["date", "amount"])
    out["type"] = out["amount"].apply(lambda v: "expense" if v < 0 else "income")

    # tx_id: preferir external_id; se vazio, usar composto
    key = out["external_id"].fillna("").astype(str)
    mask_empty = key.eq("")
    key.loc[mask_empty] = (
        out.loc[mask_empty, "date"].astype(str)
        + "|"
        + out.loc[mask_empty, "description"].astype(str)
        + "|"
        + out.loc[mask_empty, "amount"].astype(str)
        + "|nubank"
    )
    out["tx_id"] = pd.util.hash_pandas_object(key, index=False).astype("uint64").astype(str)

    return out


def append_to_ledger(new_df: pd.DataFrame, processed_dir: Path) -> Tuple[Path, Path]:
    """
    Salva/atualiza ledger.csv e ledger.parquet (se possível).
    Deduplica por tx_id.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    csv_path = processed_dir / "ledger.csv"
    pq_path = processed_dir / "ledger.parquet"

    if csv_path.exists():
        old = pd.read_csv(csv_path)
        # tenta parse de date se vier string
        if "date" in old.columns:
            old["date"] = pd.to_datetime(old["date"], errors="coerce")
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["tx_id"], keep="first")
    else:
        combined = new_df.drop_duplicates(subset=["tx_id"], keep="first")

    combined.to_csv(csv_path, index=False, encoding="utf-8")

    # parquet é opcional
    try:
        combined.to_parquet(pq_path, index=False)
    except Exception:
        pass

    return csv_path, pq_path


def main():
    inbox = Path("inbox")
    processed = Path("data") / "processed"

    files = sorted(inbox.glob("*.ofx"))
    if not files:
        print("Nenhum .ofx encontrado em inbox/. Coloque o OFX do Nubank lá e rode novamente.")
        return

    # pega o primeiro OFX com "nubank" no nome se houver, senão o primeiro
    target = None
    for f in files:
        if "nubank" in f.name.lower():
            target = f
            break
    target = target or files[0]

    df = import_nubank_ofx(target)
    csv_path, pq_path = append_to_ledger(df, processed)

    print(f"[OK] Importado Nubank OFX: {target.name}")
    print(f"[OK] Linhas importadas: {len(df)}")
    print(f"[OK] Ledger atualizado: {csv_path}")
    if pq_path.exists():
        print(f"[OK] Ledger parquet: {pq_path}")


if __name__ == "__main__":
    main()
