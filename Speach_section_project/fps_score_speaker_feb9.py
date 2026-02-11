import pandas as pd
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================

BASE_PROCESSED = Path(r"J:\Saeed Work\Speaker_EC_Project\processed")
BASE_SCORES = Path(r"J:\Saeed Work\Speaker_EC_Project\scores")
DICT_FOLDER = Path(r"J:\Saeed Work\May 20\Dictionaries")

SPEAKERS = ["CFO", "CEO", "CFO_CEO"]

DIMS = [
    "nar_vanity",
    "nar_superiority",
    "nar_self-sufficiency",
    "nar_exploitativeness",
    "nar_exhibitionism",
    "nar_entitlement",
    "nar_authority",
]

PRONOUN_DICTIONARY = {"i", "me", "my", "mine", "myself"}
WINDOW_SIZE = 10

COUNTRIES = [
    p.name for p in (BASE_PROCESSED / "CEO").iterdir()
    if p.is_dir() and p.name != "USA"
]

ID2FIRM_PATH = Path(
    r"J:\Saeed Work\Speaker_EC_Project\id2firms_parsed_feb8_no_update.csv"
)


# =========================================================
# HELPERS
# =========================================================

def load_dictionary(dim):
    df = pd.read_csv(DICT_FOLDER / f"{dim}.csv")
    return set(
        df.stack()
          .dropna()
          .str.lower()
          .str.strip()
    )


def calculate_combination_score(tokens, domain_dict):
    total_tokens = len(tokens)
    if total_tokens == 0:
        return 0.0

    pron_positions = [
        i for i, w in enumerate(tokens) if w in PRONOUN_DICTIONARY
    ]

    score = 0
    for pos in pron_positions:
        start = max(0, pos - WINDOW_SIZE)
        end = min(total_tokens, pos + WINDOW_SIZE + 1)
        context = tokens[start:end]

        if any(w in domain_dict for w in context):
            score += 1

    return score / total_tokens * 100


def process_docs(doc_path, id_path, domain_dict):
    rows = []

    with open(doc_path, encoding="utf-8") as f_doc, \
         open(id_path, encoding="utf-8") as f_id:

        for text, sid in zip(f_doc, f_id):
            text = text.strip().lower()
            if not text:
                continue

            doc_id = sid.strip().split("_")[0]
            tokens = text.split()

            rows.append({
                "Doc_ID": doc_id,
                "document_length": len(tokens),
                "score": calculate_combination_score(tokens, domain_dict),
            })

    return pd.DataFrame(rows)

# =========================================================
# MAIN
# =========================================================

def main():
    id2firm = pd.read_csv(ID2FIRM_PATH)

    id2firm["File Name"] = (
            id2firm["File Name"]
            .astype(str)
            .str.replace(".txt", "", regex=False)
        )
    
    id2firm.rename(columns={"document_id":"GVKEY"}, inplace=True)

    for country in COUNTRIES:
        print(f"\nProcessing country: {country}")

        id2firm_c = id2firm[id2firm["country"] == country]
        

        for speaker in SPEAKERS:
            print(f"  Speaker: {speaker}")
            
            doc_path = BASE_PROCESSED / speaker / country / "bigram" / "documents.txt"
            id_path = BASE_PROCESSED / speaker / country / "unigram" / "document_sent_ids.txt"

            if not doc_path.exists() or not id_path.exists():
                print("    ⏭️ Skipped (missing input)")
                continue

            out_dir = BASE_SCORES / speaker / country
            out_dir.mkdir(parents=True, exist_ok=True)

            for dim in DIMS:
                print(f"    DIM: {dim}")

                domain_dict = load_dictionary(dim)

                df = process_docs(
                    doc_path,
                    id_path,
                    domain_dict
                )

                if df.empty:
                    print("      ⏭️ No data")
                    continue

                # sentence → document aggregation
                df_doc = (
                    df
                    .groupby("Doc_ID", as_index=False)
                    .agg({
                        "document_length": "sum",
                        "score": "sum"
                    })
                )

                # -----------------------------------------
                # Merge with unified id2firm
                # -----------------------------------------
                df_doc = df_doc.merge(
                    id2firm_c,
                    how="left",
                    #on="Doc_ID"
                    left_on="Doc_ID",
                    right_on="File Name"
                )


                out_file = (
                    out_dir
                    / f"combination_scores_pron_{WINDOW_SIZE}w_{dim}.csv"
                )

                df_doc.to_csv(
                    out_file,
                    index=False,
                    float_format="%.4f"
                )

                print(f"      Saved: {out_file.name}")

    print("\n🎯 Combination scores finished for all DIMs, speakers, and countries.")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
