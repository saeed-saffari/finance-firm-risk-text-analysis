import re
from collections import defaultdict
from pathlib import Path
import global_options

def combine_sentences(doc_path, id_path, output_path, limit=None, remove_ner=True):
    with open(doc_path, "r", encoding="utf-8") as f:
        sentences = f.readlines()
    with open(id_path, "r", encoding="utf-8") as f:
        sent_ids = f.readlines()

    assert len(sentences) == len(sent_ids), f"Mismatch: {len(sentences)} lines vs {len(sent_ids)} IDs"

    firm_docs = defaultdict(list)
    for sid, sentence in zip(sent_ids, sentences):
        firm_id = sid.strip().split("_")[0]
        firm_docs[firm_id].append(sentence.strip())

    selected_firms = list(firm_docs.keys())
    if limit:
        selected_firms = selected_firms[:limit]

    with open(output_path, "w", encoding="utf-8") as f:
        for firm_id in selected_firms:
            combined = " ".join(firm_docs[firm_id])
            f.write(f"{firm_id}\t{combined.strip()}\n")

    print(f"✅ Saved combined documents for {len(selected_firms)} firm(s) → {output_path}")

if __name__ == "__main__":
    input_base = Path(global_options.DATA_FOLDER, "input")
    all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]#[:2]
    #all_countries = ['USA']

    for country in all_countries:
        print(f"\n🌍 Combining firm-level text for {country}")
        global_options.set_country_paths(country)

        doc_path = global_options.PROCESSED_FOLDER / "bigram" / "documents.txt"
        id_path = global_options.PROCESSED_FOLDER / "parsed" / "document_sent_ids.txt"
        output_path = global_options.PROCESSED_FOLDER / "bigram" / "documents_firm.txt"

        if not doc_path.exists() or not id_path.exists():
            print(f"⏭️ Skipping {country}: missing inputs")
            continue

        combine_sentences(doc_path, id_path, output_path)
