import os
import pandas as pd

# Root folder that contains all countries
base_path = r"J:\Saeed Work\May 20\data\output_21Aug"
#output_file = os.path.join(base_path, "all_countries_combined_scores.csv")
#output_file = r"J:\Saeed Work\May 20\data\report_aug21\all_countries_combined_scores_toxicity.csv"
#output_file = r"J:\Saeed Work\May 20\data\report_aug21\all_countries_combined_scores_sexual_harassment.csv"
#output_file = r"J:\Saeed Work\May 20\data\report_aug21\all_countries_combined_scores_DEI_JAR.csv"
#output_file = r"J:\Saeed Work\May 20\data\report_aug21\all_countries_combined_scores_DEI_GD.csv"
#output_file = r"J:\Saeed Work\May 20\data\report_aug21\all_countries_combined_scores_DEI_Comprehensive.csv"
output_file = r"J:\Saeed Work\May 20\data\report_aug21\all_countries_combined_scores_DEI_black.csv"






# Create a list to store all country DataFrames
all_dfs = []

# Loop through all country folders
for country in os.listdir(base_path):
    country_path = os.path.join(base_path, country)
    country_file = os.path.join(country_path, f"{country}_merged_scores_DEI_Comprehensive.csv")

    if not os.path.isfile(country_file):
        continue

    try:
        df = pd.read_csv(country_file)
        df["country"] = country  # Add country column
        all_dfs.append(df)
        print(f"✅ Processed: {country}")
    except Exception as e:
        print(f"❌ Error in {country}: {e}")

# Concatenate all country data
if all_dfs:
    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df.to_csv(output_file, index=False)
    merged_df.to_stata(output_file.replace(".csv", ".dta"), write_index=False)

    print(f"\n✅ All countries merged → {output_file}")
else:
    print("⚠️ No data to merge.")
