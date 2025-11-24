import re
import pandas as pd
from pathlib import Path
import global_options

# Shared dictionary path
#DICTIONARY_CSV = Path(r"J:\Saeed Work\May 20\Dictionaries\Exteded_Keywords_Demers_Najah\expanded_dict.csv")
DICTIONARY_CSV = Path(r"J:\Saeed Work\May 20\Dictionaries\DEI_JAR.csv")


def load_risk_words_from_txt(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    # split by whitespace and lowercase all terms
    risk_words = set(word.lower().strip() for word in content.split())
    return risk_words

# RISK_DICTIONARY = set([
#     "risk", "uncertainty", "volatility", "exposure", "financial_loss", 
#     "instability", "disruption", "downside", "liquidity", "restructuring",
#     "liquidity_risk", "loss_revenue", "headwind", "challenge", "unexpected",
#     "loss", "shortfall", "fluctuate", "risk_factor"
# ])

RISK_WORDS_TXT = Path(r"J:\Saeed Work\May 20\risk_words.txt")
RISK_DICTIONARY = load_risk_words_from_txt(RISK_WORDS_TXT)


WINDOW_SIZE = 10  # words around risk

def load_risk_words_from_txt(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    # split by whitespace and lowercase all terms
    risk_words = set(word.lower().strip() for word in content.split())
    return risk_words

def load_domain_dictionaries(csv_path):
    df = pd.read_csv(csv_path)
    dictionaries = {}
    for col in df.columns:
        dictionaries[col.lower()] = set(df[col].dropna().str.lower().str.strip())
    return dictionaries

def calculate_risk_score(ngrams, domain_dict, risk_dict, window_size):
    total_ngrams = len(ngrams)
    if total_ngrams == 0:
        return 0
    risk_positions = [i for i, word in enumerate(ngrams) if word in risk_dict]
    score = 0
    for pos in risk_positions:
        start = max(0, pos - window_size)
        end = min(total_ngrams, pos + window_size + 1)
        context_window = ngrams[start:end]
        if any(w in domain_dict for w in context_window):
            score += 1
    return score / total_ngrams * 100

# def process_firm_docs(file_path, domain_dictionaries):
#     results = []
#     with open(file_path, "r", encoding="utf-8") as f:
#         for line in f:
#             if "\t" not in line:
#                 continue
#             file_id, text = line.strip().split("\t", 1)
#             tokens = text.lower().split()
 
#             result = {"file_id": file_id}
#             for domain, domain_dict in domain_dictionaries.items():
#                 result[f"{domain}_risk"] = calculate_risk_score(tokens, domain_dict, RISK_DICTIONARY, WINDOW_SIZE)
#             results.append(result)
#     return pd.DataFrame(results)


def process_firm_docs(file_path, domain_dictionaries):
    results = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            parts = line.strip().split("\t", 1)
            if len(parts) != 2:
                print(f"[Warning] Skipped malformed line {line_num} in {file_path}: {line.strip()}")
                continue
            file_id, text = parts
            tokens = text.lower().split()

            result = {"file_id": file_id}
            for domain, domain_dict in domain_dictionaries.items():
                result[f"{domain}_risk"] = calculate_risk_score(tokens, domain_dict, RISK_DICTIONARY, WINDOW_SIZE)
            results.append(result)
    return pd.DataFrame(results)


if __name__ == "__main__":
    input_base = Path(global_options.DATA_FOLDER, "input")
    all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]
    #all_countries = [f.name for f in input_base.iterdir() if f.is_dir() and f.name != ["AUS"]]
    #all_countries = ['USA']

    domain_dictionaries = load_domain_dictionaries(DICTIONARY_CSV)

    for country in all_countries:
        print(f"\n🌍 Processing risk words for {country}")
        global_options.set_country_paths(country)

        input_path = global_options.PROCESSED_FOLDER / "bigram" / "documents_firm.txt"
        score_output_path = global_options.OUTPUT_FOLDER / "scores" / "word_combination_scores_DEI_JAR.csv"
        merged_output_path = global_options.OUTPUT_FOLDER / "scores" / "firm_level_combination_scores_merged_DEI_JAR.csv"
        #agg_output_path = global_options.OUTPUT_FOLDER / "scores" / "firm_year_combination_scores.csv"
        agg_output_path = global_options.OUTPUT_FOLDER / "firm_year_combination_scores_DEI_JAR.csv"

        id2firm_path = Path(global_options.DATA_FOLDER, "input", country, "id2firms.csv")

        if not input_path.exists():
            print(f"⏭️ Skipping {country}: no input file")
            continue

        df_scores = process_firm_docs(input_path, domain_dictionaries)
        df_scores.to_csv(score_output_path, index=False)
        print(f"✅ Saved base scores to {score_output_path}")

        if id2firm_path.exists():
            df_firm = pd.read_csv(id2firm_path).drop(columns=["date_full", "month", "day"])
            merged = df_scores.merge(df_firm, how="left", left_on="file_id", right_on="File Name")
            #merged.to_csv(merged_output_path, index=False)
            #print(f"✅ Saved merged firm-level scores to {merged_output_path}")

            # Aggregate
            agg_df = merged.groupby(["document_id", "year"]).mean(numeric_only=True).reset_index()
            agg_df = agg_df.rename(columns={"document_id": "GVKEY"})
            agg_df.to_csv(agg_output_path, index=False)
            print(f"✅ Saved aggregated gvkey-year scores to {agg_output_path}")
        else:
            print(f"⚠️ Missing id2firms.csv for {country}, skipping merge + aggregation.")

