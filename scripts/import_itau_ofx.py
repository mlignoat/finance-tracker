from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


def _parse_ofx_sgml(text: str) -> List[Dict[str, str]]:
    """
    Parser simples para OFX (SGML-like). Extrai <STMTTRN>...</STMTTRN>.
    """
    blocks = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", text, flags=re.S | re.I)
    rows: List[Dict[str, str]] = []

    for b in blocks:
        def tag(name: str) -> str:
            m = re.search(rf"<{name}>([^\r\n<]+)", b, flags=re.I)
            return m.group(1).strip() if m else ""

        dtposted = tag("DTPOSTED")
        trnamt = tag("TRNAMT")
        memo = tag("MEMO") or tag("NAME")
        fitid = tag("FITID")  # às vezes vem vazio no Itaú

        # alguns OFX têm <CHECKNUM> ou <REFNUM> úteis
        checknum = tag("CHECKNUM")
        refnum = tag("REFNUM")

        rows.append({
            "DTPOSTED": dtposted,
            "TRNAMT": trnamt,
            "MEMO": memo,
            "FITID": fitid,
            "CHECKNUM": checknum,
            "REFNUM": refnum,
        })

    return rows


def _coerce_date(dtposted: str) -> pd.Timestamp:
    m = re.match(r"(\d{4})(\d{2})(\d{2})", dtposted or "")
    if not m:
        return pd.NaT
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return pd.to_datetime(f"{y}-{mo}-{d}", errors="coerce")


def _coerce_amount(x: str) -> float:
    try:
        return float(str(x).strip())
    except Exception:
        return float("nan")


def import_itau_ofx(ofx_path: Path) -> pd.DataFrame:
    """
    Lê OFX Itaú e devolve DataFrame padronizado:
    date, description, amount, source, external_id, type, tx_id, file_name
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
        "source": "itau",
        # external_id: prioriza FITID; senão usa REFNUM/CHECKNUM
        "external_id": (
            df["FITID"].fillna("").astype(str).str.strip()
        ),
        "file_name": ofx_path.name,
    })

    # fallback external_id se FITID vazio
    fallback = (
        df.get("REFNUM", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
        + "|"
        + df.get("CHECKNUM", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    )
    mask_empty = out["external_id"].fillna("").eq("")
    out.loc[mask_empty, "external_id"] = fallback.loc[mask_empty].astype(str)

    out = out.dropna(subset=["date", "amount"])
    out["type"] = out["amount"].apply(lambda v: "expense" if v < 0 else "income")

    # tx_id: se external_id útil, usar; senão composto
    key = out["external_id"].fillna("").astype(str)
    mask_key_empty = key.eq("") | key.eq("|")  # caso REF|CHECK vazio
    key.loc[mask_key_empty] = (
        out.loc[mask_key_empty, "date"].astype(str)
        + "|"
        + out.loc[mask_key_empty, "description"].astype(str)
        + "|"
        + out.loc[mask_key_empty, "amount"].astype(str)
        + "|itau"
    )
    out["tx_id"] = pd.util.hash_pandas_object(key, index=False).astype("uint64").astype(str)

    return out


def append_to_ledger(new_df: pd.DataFrame, processed_dir: Path) -> Tuple[Path, Path]:
    """
    Atualiza ledger.csv e ledger.parquet (se possível). Dedup por tx_id (string).
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    csv_path = processed_dir / "ledger.csv"
    pq_path = processed_dir / "ledger.parquet"

    if csv_path.exists():
        old = pd.read_csv(csv_path, dtype={"tx_id": "string", "external_id": "string"})
        if "date" in old.columns:
            old["date"] = pd.to_datetime(old["date"], errors="coerce")
        combined = pd.concat([old, new_df], ignore_index=True)
        combined["tx_id"] = combined["tx_id"].astype(str)
        combined = combined.drop_duplicates(subset=["tx_id"], keep="first")
    else:
        combined = new_df.drop_duplicates(subset=["tx_id"], keep="first")

    combined.to_csv(csv_path, index=False, encoding="utf-8")
    try:
        combined.to_parquet(pq_path, index=False)
    except Exception:
        pass

    return csv_path, pq_path


def main():
    inbox = Path("inbox")
    processed = Path("data") / "processed"

    # pega o OFX do Itaú pelo nome (itau) ou usa o primeiro .ofx se só tiver ele
    files = sorted(inbox.glob("*.ofx"))
    if not files:
        print("Nenhum .ofx encontrado em inbox/. Coloque o OFX do Itaú lá e rode novamente.")
        return

    target = None
    for f in files:
        name = f.name.lower()
        if "itau" in name or "itaú" in name or "extrato" in name:
            target = f
            break
    target = target or files[0]

    df = import_itau_ofx(target)
    csv_path, pq_path = append_to_ledger(df, processed)

    print(f"[OK] Importado Itaú OFX: {target.name}")
    print(f"[OK] Linhas importadas: {len(df)}")
    print(f"[OK] Ledger atualizado: {csv_path}")
    if pq_path.exists():
        print(f"[OK] Ledger parquet: {pq_path}")


if __name__ == "__main__":
    main()
