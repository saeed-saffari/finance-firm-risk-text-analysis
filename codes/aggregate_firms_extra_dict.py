"""Aggregate scores to firm-year level (optional)
Scores are adjusted by document length (100*score/length)
"""

import global_options
import pandas as pd
from pathlib import Path
import time

# print("Aggregating scores to firms and adjusting by document lengths.")

# id2firm = pd.read_csv(str(Path(global_options.DATA_FOLDER, "input", "id2firms.csv")))

# methods = ["TF", "TFIDF", "WFIDF"]
# start_method = time.time()
# for method in methods:
#     scores = pd.read_csv(
#         str(
#             Path(global_options.OUTPUT_FOLDER, "scores", "scores_{}.csv".format(method))
#         )
#     )
#     scores = scores.merge(
#         id2firm, how="left", left_on=["Doc_ID"], right_on="File Name"
#     ).drop(["Doc_ID"], axis=1)
#     for dim in global_options.DIMS:
#         scores[dim] = 100 * scores[dim] / scores["document_length"]
#     scores.groupby(["document_id", "year"]).mean()
#     scores.sort_values(by=["document_id", "year"], ascending=[True, True])
#     scores.to_csv(
#         str(
#             Path(
#                 global_options.OUTPUT_FOLDER,
#                 "scores",
#                 "firm_scores_{}.csv".format(method),
#             )
#         ),
#         index=False,
#         float_format="%.4f",
#     )
# print(f"Time for {method}: {round(time.time() - start_method, 2)} seconds")




print("Aggregating scores to firms and adjusting by document lengths.\n")
input_base = Path(global_options.DATA_FOLDER, "input_21Aug")
all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]#[:11]

for country in all_countries:
    global_options.set_country_paths(country)

    id2firm_path = Path(global_options.DATA_FOLDER, "input_21Aug", country, "id2firms_update_Aug21.csv")

    if not id2firm_path.exists():
        print(f"⏭️ Skipping {country}: missing id2firms.csv")
        continue

    id2firm = pd.read_csv(id2firm_path)
    print(f"✅ Processing {country}")

    methods = ["TF", "TFIDF", "WFIDF"]
    for method in methods:
        score_file = global_options.OUTPUT_FOLDER / "scores" / f"scores_{method}_DEI_black.csv"
        if not score_file.exists():
            print(f"   ⚠️ Skipping {method}: scores file missing.")
            continue

        scores = pd.read_csv(score_file)

        # Merge with firm data
        scores = scores.merge(
            id2firm, how="left", left_on=["Doc_ID"], right_on="File Name"
        ).drop(["Doc_ID"], axis=1)

        # Adjust by document length
        for dim in global_options.DIMS:
            scores[dim] = 100 * scores[dim] / scores["document_length"]

        # Save aggregated score
        output_file = global_options.OUTPUT_FOLDER / "scores" / f"firm_scores_{method}_DEI_black.csv"
        scores.to_csv(output_file, index=False, float_format="%.4f")

        print(f"   📄 Saved: firm_scores_{method}.csv")
