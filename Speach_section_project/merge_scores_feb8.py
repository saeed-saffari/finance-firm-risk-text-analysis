import pandas as pd
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================

BASE_SCORES = Path(r"J:\Saeed Work\Speaker_EC_Project\scores")

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

# Countries inferred safely from score folders

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
        print(f"\nProcessing country: {country}")

        for speaker in SPEAKERS:
            base_dir = BASE_SCORES / speaker / country / "firm_scores"
            if not base_dir.exists():
                print(f"  {speaker}: firm_scores missing, skipped")
                continue

            print(f"  Speaker: {speaker}")

            for dim in DIMS:

                tf_path = base_dir / f"firm_scores_TF_{dim}.csv"
                tfidf_path = base_dir / f"firm_scores_TFIDF_{dim}.csv"
                wfidf_path = base_dir / f"firm_scores_WFIDF_{dim}.csv"

                if not (tf_path.exists() and tfidf_path.exists() and wfidf_path.exists()):
                    continue

                # -----------------------------
                # Load
                # -----------------------------
                tf = pd.read_csv(tf_path)
                tfidf = pd.read_csv(tfidf_path)
                wfidf = pd.read_csv(wfidf_path)

                # -----------------------------
                # Rename score columns
                # -----------------------------
                tf = tf.rename(columns={dim: f"{dim}_tf"})
                tfidf = tfidf.rename(columns={dim: f"{dim}_tfidf"})
                wfidf = wfidf.rename(columns={dim: f"{dim}_wfidf"})

                # -----------------------------
                # Merge on firm-year
                # -----------------------------
                merged = (
                    tf
                    .merge(
                        tfidf[["GVKEY", "year", f"{dim}_tfidf"]],
                        on=["GVKEY", "year"],
                        how="left"
                    )
                    .merge(
                        wfidf[["GVKEY", "year", f"{dim}_wfidf"]],
                        on=["GVKEY", "year"],
                        how="left"
                    )
                )

                # -----------------------------
                # Reorder columns
                # -----------------------------
                ordered_cols = [
                    "GVKEY",
                    "year",
                    f"{dim}_tf",
                    f"{dim}_tfidf",
                    f"{dim}_wfidf",
                ]

                merged = merged[ordered_cols]

                # -----------------------------
                # Save
                # -----------------------------
                out_file = (
                    BASE_SCORES
                    / speaker
                    / country
                    / f"{speaker}_{country}_merged_{dim}.csv"
                )

                merged.to_csv(
                    out_file,
                    index=False,
                    float_format="%.4f"
                )

                print(f"    Saved: {out_file.name}")

    print("\n🎯 Merging TF / TFIDF / WFIDF completed successfully.")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
