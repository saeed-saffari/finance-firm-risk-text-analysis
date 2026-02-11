import pandas as pd
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================

BASE_SCORES = Path(r"J:\Saeed Work\Speaker_EC_Project\scores")
#ID2FIRM_BASE = Path(r"J:\Saeed Work\May 20\data\input_21Aug")

SPEAKERS = ["CFO", "CEO", "CFO_CEO"]
METHODS = ["TF", "TFIDF", "WFIDF"]

DIMS = [
    "nar_vanity",
    "nar_superiority",
    "nar_self-sufficiency",
    "nar_exploitativeness",
    "nar_exhibitionism",
    "nar_entitlement",
    "nar_authority",
]

# Countries inferred from score folders (safe)

ID2FIRM_PATH = Path(
    r"J:\Saeed Work\Speaker_EC_Project\id2firms_parsed_feb8_no_update.csv"
)

COUNTRIES = [
    p.name for p in Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CFO_CEO").iterdir()
    if p.is_dir() and p.name != "USA" and 
     p.name != "GHA" and # no CFO and CEO
    # p.name != "JOR" and # no CFO
    # p.name != "LIE" and # no CFO
    # p.name != "LKA" and # no CFO
     p.name != "VNM" #and # no CFO and CEO
    # p.name != "BRB" and # no CEO
    # p.name != "MU.OQ" # no CEO
]

# =========================================================
# MAIN
# =========================================================

def main():

    for country in COUNTRIES:

        # id2firm_path = ID2FIRM_BASE / country / "id2firms_update_Aug21.csv"
        # if not id2firm_path.exists():
        #     print(f"Skipping {country}: missing id2firms file")
        #     continue

        #id2firm = pd.read_csv(id2firm_path)
        id2firm = pd.read_csv(ID2FIRM_PATH)
        id2firm["File Name"] = (
            id2firm["File Name"]
            .astype(str)
            .str.replace(".txt", "", regex=False)
        )

        id2firm.rename(columns={"document_id":"GVKEY"}, inplace=True)
        print(f"\nProcessing country: {country}")

        for speaker in SPEAKERS:

            score_dir = BASE_SCORES / speaker / country
            if not score_dir.exists():
                print(f"  {speaker}: no score folder, skipped")
                continue

            out_dir = score_dir / "firm_scores"
            out_dir.mkdir(exist_ok=True)

            print(f"  Speaker: {speaker}")

            for dim in DIMS:
                for method in METHODS:

                    score_file = score_dir / f"scores_{method}_{dim}.csv"
                    if not score_file.exists():
                        continue

                    scores = pd.read_csv(score_file)

                    # -----------------------------------------
                    # Merge with firm metadata
                    # -----------------------------------------
                    scores = scores.merge(
                        id2firm,
                        how="left",
                        left_on="Doc_ID",
                        right_on="File Name"
                    )

                    # -----------------------------------------
                    # Normalize by document length
                    # -----------------------------------------
                    scores[dim] = (
                        100 * scores[dim] / scores["document_length"]
                    )

                    # -----------------------------------------
                    # Aggregate to firm-year
                    # -----------------------------------------
                    firm_scores = (
                        scores
                        .groupby(
                            ["GVKEY", "year"],
                            as_index=False
                        )[dim]
                        .mean()
                        .sort_values(["GVKEY", "year"])
                    )

                    # -----------------------------------------
                    # Save
                    # -----------------------------------------
                    out_file = (
                        out_dir
                        / f"firm_scores_{method}_{dim}.csv"
                    )

                    firm_scores.to_csv(
                        out_file,
                        index=False,
                        float_format="%.4f"
                    )

                    print(
                        f"    Saved: {speaker} | {method} | {dim}"
                    )

    print("\n🎯 Firm-level aggregation completed successfully.")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
