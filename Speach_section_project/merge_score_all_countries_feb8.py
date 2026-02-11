import pandas as pd
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================

BASE_SCORES = Path(r"J:\Saeed Work\Speaker_EC_Project\scores")
OUT_BASE = Path(r"J:\Saeed Work\Speaker_EC_Project\merged_score")

SPEAKERS = ["CEO", "CFO", "CFO_CEO"]

DIMS = [
    "nar_vanity",
    "nar_superiority",
    "nar_self-sufficiency",
    "nar_exploitativeness",
    "nar_exhibitionism",
    "nar_entitlement",
    "nar_authority",
]

# =========================================================
# MAIN
# =========================================================

def main():

    for speaker in SPEAKERS:

        speaker_dir = BASE_SCORES / speaker
        if not speaker_dir.exists():
            print(f"\n⏭️ Skipping speaker (no folder): {speaker}")
            continue

        out_dir = OUT_BASE / speaker
        out_dir.mkdir(parents=True, exist_ok=True)

        countries = [
            p.name for p in speaker_dir.iterdir()
            if p.is_dir() and p.name != "USA"
        ]

        print(f"\n👤 Speaker: {speaker}")
        print(f"Countries found: {countries}")

        for dim in DIMS:

            all_frames = []

            for country in countries:

                file_path = (
                    speaker_dir
                    / country
                    / f"{speaker}_{country}_merged_{dim}.csv"
                )

                if not file_path.exists():
                    continue

                df = pd.read_csv(file_path)

                # ensure country column exists
                if "country" not in df.columns:
                    df["country"] = country

                all_frames.append(df)

                print(f"  Loaded: {file_path.name}")

            if not all_frames:
                print(f"  ⏭️ No files found for {dim}")
                continue

            merged_df = pd.concat(all_frames, ignore_index=True)

            out_file = out_dir / f"{speaker}_ALL_merged_{dim}.csv"
            merged_df.to_csv(out_file, index=False, float_format="%.4f")

            print(f"  ✅ Saved: {out_file.name}")

    print("\n🎯 All speakers and dimensions merged successfully.")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
