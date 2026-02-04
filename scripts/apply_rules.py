import re
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = PROJECT_ROOT / "rules" / "rules.csv"


def load_rules(path: Path) -> pd.DataFrame:
    rules = pd.read_csv(path)
    rules["priority"] = pd.to_numeric(rules["priority"], errors="coerce").fillna(9999).astype(int)
    rules = rules.sort_values("priority")
    # compila regex (case-insensitive)
    rules["regex"] = rules["pattern"].astype(str).apply(lambda p: re.compile(p, re.I))
    return rules


def apply_rules(df: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "category" not in df.columns:
        df["category"] = "Uncategorized"
    if "subcategory" not in df.columns:
        df["subcategory"] = "Uncategorized"

    desc = df["description"].astype(str)

    for _, r in rules.iterrows():
        rx = r["regex"]
        cat = str(r.get("category", "")).strip()
        sub = str(r.get("subcategory", "")).strip()
        hint = str(r.get("type_hint", "")).strip().lower()

        if not cat:
            continue

        m = desc.apply(lambda x: bool(rx.search(x)))
        # só aplica se ainda estiver sem categoria (pra respeitar prioridade)
        m = m & (df["category"].isin(["Uncategorized", "", None]) | df["category"].isna())

        df.loc[m, "category"] = cat
        df.loc[m, "subcategory"] = sub if sub else cat

        if hint in ["expense", "income", "transfer", "investment"]:
            df.loc[m, "type"] = hint

    return df


def main():
    ledger_path = PROJECT_ROOT / "data" / "processed" / "ledger.csv"
    if not ledger_path.exists():
        print("Não encontrei data/processed/ledger.csv")
        return

    df = pd.read_csv(ledger_path, dtype={"tx_id": "string", "external_id": "string"})
    rules = load_rules(RULES_PATH)
    out = apply_rules(df, rules)

    out.to_csv(ledger_path, index=False, encoding="utf-8")
    print(f"[OK] Regras aplicadas no ledger: {ledger_path}")


if __name__ == "__main__":
    main()
