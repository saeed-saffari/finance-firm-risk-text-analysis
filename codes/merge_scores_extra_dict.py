import os
import pandas as pd

# Root folder that contains all countries
base_path = r"J:\Saeed Work\May 20\data\output_21Aug"

# Loop through all country folders
for country in os.listdir(base_path):
    country_path = os.path.join(base_path, country)
    scores_path = os.path.join(country_path, "scores")
    
    if not os.path.isdir(scores_path):
        continue  # Skip if scores folder doesn't exist

    print(f"Processing: {country}")

    try:
        # Load firm score files
        firm_tf = pd.read_csv(os.path.join(scores_path, "firm_scores_TF_DEI_black.csv"))
        firm_tfidf = pd.read_csv(os.path.join(scores_path, "firm_scores_TFIDF_DEI_black.csv"))
        firm_wfidf = pd.read_csv(os.path.join(scores_path, "firm_scores_WFIDF_DEI_black.csv"))

        # Load document score files
        score_tf = pd.read_csv(os.path.join(scores_path, "scores_TF_DEI_black.csv"))
        score_tfidf = pd.read_csv(os.path.join(scores_path, "scores_TFIDF_DEI_black.csv"))
        score_wfidf = pd.read_csv(os.path.join(scores_path, "scores_WFIDF_DEI_black.csv"))

        # Metadata columns (including document_length)
        meta_cols = ['File Name', 'document_id', 'country', 'date_full', 'year', 'month', 'day', 'document_length']
        metadata = firm_tf[meta_cols].copy()

        # Drop metadata columns
        firm_tf_scores = firm_tf.drop(columns=meta_cols)
        firm_tfidf_scores = firm_tfidf.drop(columns=meta_cols)
        firm_wfidf_scores = firm_wfidf.drop(columns=meta_cols)

        # Rename firm columns
        firm_tf_scores.columns = [col + "_firm_tf" for col in firm_tf_scores.columns]
        firm_tfidf_scores.columns = [col + "_firm_tfidf" for col in firm_tfidf_scores.columns]
        firm_wfidf_scores.columns = [col + "_firm_wfidf" for col in firm_wfidf_scores.columns]

        # Combine firm scores
        firm_all = pd.concat([metadata, firm_tf_scores, firm_tfidf_scores, firm_wfidf_scores], axis=1)

        # Rename document score columns
        score_tf = score_tf.rename(columns=lambda x: x + "_tf" if x != "Doc_ID" else x)
        score_tfidf = score_tfidf.rename(columns=lambda x: x + "_tfidf" if x != "Doc_ID" else x)
        score_wfidf = score_wfidf.rename(columns=lambda x: x + "_wfidf" if x != "Doc_ID" else x)

        # Merge document score files
        doc_scores = score_tf.merge(score_tfidf, on="Doc_ID", how="outer")
        doc_scores = doc_scores.merge(score_wfidf, on="Doc_ID", how="outer")

        # Align keys
        firm_all["File Name"] = firm_all["File Name"].astype(str).str.strip()
        doc_scores["Doc_ID"] = doc_scores["Doc_ID"].astype(str).str.strip()

        # Final merge
        final = firm_all.merge(doc_scores, left_on="File Name", right_on="Doc_ID", how="left")

        # Save merged file
        output_file = os.path.join(country_path, f"{country}_merged_scores_DEI_black.csv")
        final.to_csv(output_file, index=False)
        print(f"✅ Saved: {output_file}")

    except Exception as e:
        print(f"❌ Error processing {country}: {e}")