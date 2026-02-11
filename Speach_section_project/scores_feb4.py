import pandas as pd
import pickle
from pathlib import Path
from collections import defaultdict
from multiprocessing import freeze_support

from culture import culture_dictionary, file_util

# =========================================================
# CONFIG
# =========================================================

BASE_PROCESSED = Path(r"J:\Saeed Work\Speaker_EC_Project\processed")
BASE_SCORES = Path(r"J:\Saeed Work\Speaker_EC_Project\scores")

DICT_BASE = Path(r"J:\Saeed Work\May 20\Dictionaries")

SPEAKERS = ["CFO", "CEO", "CFO_CEO"]
#SPEAKERS = ["CEO", "CFO_CEO"]


# Narcissism dimensions (explicit, clear, paper-aligned)
DIMS = [
    "nar_vanity",
    "nar_superiority",
    "nar_self-sufficiency",
    "nar_exploitativeness",
    "nar_exhibitionism",
    "nar_entitlement",
    "nar_authority",
]

# All countries automatically
# COUNTRIES = [
#     p.name for p in BASE_PROCESSED.iterdir()
#     if p.is_dir()
# ]

COUNTRIES = [
    p.name for p in Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CEO").iterdir()
    if p.is_dir() and p.name != "USA" and 
    p.name != "GHA" and # no CFO and CEO
    p.name != "JOR" and # no CFO
    p.name != "LIE" and # no CFO
    p.name != "LKA" and # no CFO
    p.name != "VNM" and # no CFO and CEO
    p.name != "BRB" and # no CEO
    p.name != "MU.OQ" # no CEO
]

#COUNTRIES = ["BRB", "MU.OQ"]
#SPEAKERS = ["CFO", "CFO_CEO"]

# =========================================================
# HELPERS
# =========================================================

def construct_doc_level_corpus(sent_file, id_file, temp_dir):
    sentences = file_util.file_to_list(sent_file)
    sent_ids = file_util.file_to_list(id_file)

    assert len(sentences) == len(sent_ids), "Sentence / ID length mismatch"

    doc_map = defaultdict(list)
    for sid, sent in zip(sent_ids, sentences):
        doc_id = sid.split("_")[0]
        doc_map[doc_id].append(sent)

    corpus = [" ".join(v) for v in doc_map.values()]
    doc_ids = list(doc_map.keys())

    temp_dir.mkdir(parents=True, exist_ok=True)

    with open(temp_dir / "corpus_doc_level.pickle", "wb") as f:
        pickle.dump(corpus, f)

    with open(temp_dir / "doc_ids.pickle", "wb") as f:
        pickle.dump(doc_ids, f)

    return corpus, doc_ids


def calculate_df(corpus, temp_dir):
    df_dict = defaultdict(int)
    for doc in corpus:
        for w in set(doc.split()):
            df_dict[w] += 1

    with open(temp_dir / "doc_freq.pickle", "wb") as f:
        pickle.dump(df_dict, f)

    return df_dict


def load_dictionary(dim_name):
    dict_path = DICT_BASE / f"{dim_name}.csv"
    df = pd.read_csv(dict_path)

    return {
        dim_name: set(
            df.stack()
              .dropna()
              .str.lower()
              .str.strip()
        )
    }

# =========================================================
# MAIN
# =========================================================

def main():

    for dim in DIMS:
        print(f"\n==============================")
        print(f"📘 Dictionary: {dim}")
        print(f"==============================")

        nar_words = load_dictionary(dim)

        for speaker in SPEAKERS:
            for country in COUNTRIES:

                print(f"\nScoring | {speaker} | {country}")

                sent_path = BASE_PROCESSED / speaker / country / "bigram" / "documents.txt"
                id_path = BASE_PROCESSED / speaker / country / "unigram" / "document_sent_ids.txt"

                if not sent_path.exists() or not id_path.exists():
                    print("  ⏭️ Skipped (missing input)")
                    continue

                score_dir = BASE_SCORES / speaker / country
                temp_dir = score_dir / "temp"
                score_dir.mkdir(parents=True, exist_ok=True)

                # -------------------------------------------------
                # 1. sentence → document
                # -------------------------------------------------
                corpus, doc_ids = construct_doc_level_corpus(
                    sent_path, id_path, temp_dir
                )

                # -------------------------------------------------
                # 2. document frequency
                # -------------------------------------------------
                df_dict = calculate_df(corpus, temp_dir)
                N_doc = len(corpus)

                # -------------------------------------------------
                # 3. TF
                # -------------------------------------------------
                scores_tf = culture_dictionary.score_tf(
                    documents=corpus,
                    document_ids=doc_ids,
                    expanded_words=nar_words,
                )

                scores_tf.to_csv(
                    score_dir / f"scores_TF_{dim}.csv",
                    index=False
                )

                # -------------------------------------------------
                # 4. TFIDF
                # -------------------------------------------------
                scores_tfidf, _ = culture_dictionary.score_tf_idf(
                    documents=corpus,
                    document_ids=doc_ids,
                    N_doc=N_doc,
                    method="TFIDF",
                    expanded_words=nar_words,
                    df_dict=df_dict,
                )

                scores_tfidf.to_csv(
                    score_dir / f"scores_TFIDF_{dim}.csv",
                    index=False
                )

                # -------------------------------------------------
                # 5. WFIDF
                # -------------------------------------------------
                scores_wfidf, _ = culture_dictionary.score_tf_idf(
                    documents=corpus,
                    document_ids=doc_ids,
                    N_doc=N_doc,
                    method="WFIDF",
                    expanded_words=nar_words,
                    df_dict=df_dict,
                )

                scores_wfidf.to_csv(
                    score_dir / f"scores_WFIDF_{dim}.csv",
                    index=False
                )

                print("  ✅ Saved TF / TFIDF / WFIDF")

    print("\n🎯 All narcissism dimensions processed successfully.")

# =========================================================
# ENTRY POINT (WINDOWS SAFE)
# =========================================================

if __name__ == "__main__":
    freeze_support()
    main()
