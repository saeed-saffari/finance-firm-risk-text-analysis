from pathlib import Path
from collections import defaultdict, Counter
import pandas as pd

# ============================================================
# Pronoun dictionaries (lowercase, unigram-compatible)
# ============================================================

FIRST_PERSON_SINGULAR = {
    "i", "me", "my", "mine", "myself"
}

OTHER_PRONOUNS = {
    "we", "us", "our", "ours", "ourselves",
    "you", "your", "yours", "yourself", "yourselves",
    "they", "them", "their", "theirs", "themselves",
    "he", "him", "his",
    "she", "her", "hers",
    "it", "its", "itself"
}

# ============================================================
# Base folders
# ============================================================

BASE_PROCESSED = Path(r"J:\Saeed Work\Speaker_EC_Project\processed")
BASE_OUTPUT = Path(r"J:\Saeed Work\Speaker_EC_Project\scores")

SPEAKERS = ["CFO", "CEO", "CFO_CEO"]

# All available countries
#COUNTRIES = [p.name for p in BASE_PROCESSED.iterdir() if p.is_dir()]

# All countries except USA
COUNTRIES = [
    p.name for p in Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CEO").iterdir()
    if p.is_dir() and p.name != "USA"
]

# ============================================================
# Main loop
# ============================================================

for country in COUNTRIES:
    print('name', country)
    for speaker in SPEAKERS:

        unigram_path = (
            BASE_PROCESSED / speaker / country / "unigram" / "documents.txt"
        )

        sent_id_path = (
            BASE_PROCESSED / speaker / country / "unigram" / "document_sent_ids.txt"
        )

        if not unigram_path.exists() or not sent_id_path.exists():
            print(f"⏭️ Skipping {country} | {speaker} (missing files)")
            continue

        output_path = (
            BASE_OUTPUT
            / speaker
            / country
            / f"{speaker.lower()}_pronoun_narcissism.csv"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # ====================================================
        # Reconstruct firm-level documents
        # ====================================================

        documents = defaultdict(list)

        with open(unigram_path, encoding="utf-8") as f_text, \
             open(sent_id_path, encoding="utf-8") as f_ids:

            for text, sid in zip(f_text, f_ids):
                doc_id = sid.strip().rsplit("_", 1)[0]
                documents[doc_id].append(text.strip())

        documents = {k: " ".join(v) for k, v in documents.items()}

        print(
            f"Loaded {len(documents):,} documents | "
            f"Country: {country} | Speaker: {speaker}"
        )

        # ====================================================
        # Compute pronoun-based metrics
        # ====================================================

        rows = []

        for doc_id, text in documents.items():
            tokens = text.split()
            counts = Counter(tokens)

            total_words = len(tokens)
            fps_count = sum(counts[p] for p in FIRST_PERSON_SINGULAR)
            other_count = sum(counts[p] for p in OTHER_PRONOUNS)
            total_pronouns = fps_count + other_count

            rows.append({
                "doc_id": doc_id,
                "country": country,
                "speaker": speaker,

                "total_words": total_words,

                "fps_count": fps_count,
                "other_pronoun_count": other_count,
                "total_pronouns": total_pronouns,

                # Shares relative to total words
                "fps_share_words": fps_count / total_words if total_words > 0 else 0,
                "other_share_words": other_count / total_words if total_words > 0 else 0,

                # Shares relative to pronouns only (key narcissism measures)
                "fps_share_of_pronouns": (
                    fps_count / total_pronouns if total_pronouns > 0 else 0
                ),
                "other_share_of_pronouns": (
                    other_count / total_pronouns if total_pronouns > 0 else 0
                ),

                # Ratio
                "fps_to_other_ratio": fps_count / (other_count + 1)
            })

        df = pd.DataFrame(rows)

        # ====================================================
        # Save
        # ====================================================

        df.to_csv(output_path, index=False)
        print(f"✅ Saved: {output_path}")

print("\n🎯 All countries and speakers processed successfully.")
