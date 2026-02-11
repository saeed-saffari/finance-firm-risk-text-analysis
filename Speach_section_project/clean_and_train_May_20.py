import datetime
import functools
import logging
import sys
from pathlib import Path

import pandas as pd
import time


import global_options
#import parse_parallel
#import parse_parallel_May20 as parse                        # saeed edit May 20 
import parse_parallel_speaker_2 as parse                        # saeed edit May 20 
from culture import culture_models, file_util, preprocess


#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
#log_file = "J:\Saeed Work\May 20\clean_and_train_log3_aug21.txt"
log_file = "J:\Saeed Work\Speaker_EC_Project\clean_and_train_log_feb2.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

CATEGORIES = ["CEO", "CFO", "CFO_CEO"]

FALLBACK_LOG = Path(
    r"J:\Saeed Work\Speaker_EC_Project\logs\bigram_fallback_countries.txt"
)
FALLBACK_LOG.parent.mkdir(parents=True, exist_ok=True)



# def clean_file(in_file, out_file):
#     """Clean the entire corpus (output from CoreNLP)

#     Args:
#         in_file (str or Path): Input corpus, each line is a sentence
#         out_file (str or Path): Output corpus
#     """
#     a_text_cleaner = preprocess.text_cleaner()

#     start_time = time.time()
#     parse.process_largefile(
#         input_file=in_file,
#         output_file=out_file,
#         input_file_ids=[
#             str(i) for i in range(file_util.line_counter(in_file))
#         ],  # fake IDs (do not need IDs for this function).
#         output_index_file=None,
#         function_name=functools.partial(a_text_cleaner.clean),
#         chunk_size=200000,
#         #encoding="utf-8",  # Ensure UTF-8 encoding for output
#     )
#     end_time = time.time()
#     logging.info(f"Finished cleaning: {datetime.datetime.now()}")
#     logging.info(f"Total time: {(end_time - start_time)/60:.4f} minutes")

def clean_file(in_file, out_file):
    """
    Speaker cleaning step.
    Each line is already a sentence.
    """
    a_text_cleaner = preprocess.text_cleaner()

    in_file = Path(in_file)
    out_file = Path(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    n = 0

    with open(in_file, encoding="utf-8", errors="ignore") as f_in, \
         open(out_file, "w", encoding="utf-8") as f_out:

        for line in f_in:
            cleaned_text, _ = a_text_cleaner.clean(line, "0")  # ← FIX
            if cleaned_text:
                f_out.write(cleaned_text + "\n")
            n += 1

            if n % 500_000 == 0:
                logging.info(f"🧹 Cleaned {n:,} sentences")

    logging.info(f"Finished cleaning {n:,} sentences")
    logging.info(f"Total time: {(time.time() - start_time)/60:.2f} minutes")


# if __name__ == '__main__':                       # saeed edit
#     # clean the parsed text (remove POS tags, stopwords, etc.)
#     clean_file(
#         in_file=Path(global_options.DATA_FOLDER, "processed", "parsed", "documents.txt"),
#         out_file=Path(global_options.DATA_FOLDER, "processed", "unigram", "documents.txt"),
#     )

#     # train and apply a phrase model to detect 2-word phrases
#     culture_models.train_bigram_model(
#         input_path=Path(
#             global_options.DATA_FOLDER, "processed", "unigram", "documents.txt"
#         ),
#         model_path=Path(global_options.MODEL_FOLDER, "phrases", "bigram.mod"),
#     )
#     culture_models.file_bigramer(
#         input_path=Path(
#             global_options.DATA_FOLDER, "processed", "unigram", "documents.txt"
#         ),
#         output_path=Path(
#             global_options.DATA_FOLDER, "processed", "bigram", "documents.txt"
#         ),
#         model_path=Path(global_options.MODEL_FOLDER, "phrases", "bigram.mod"),
#         scoring="original_scorer",
#         threshold=global_options.PHRASE_THRESHOLD,
#     )

#     # train and apply a phrase model to detect 3-word phrases
#     culture_models.train_bigram_model(
#         input_path=Path(global_options.DATA_FOLDER, "processed", "bigram", "documents.txt"),
#         model_path=Path(global_options.MODEL_FOLDER, "phrases", "trigram.mod"),
#     )
#     culture_models.file_bigramer(
#         input_path=Path(global_options.DATA_FOLDER, "processed", "bigram", "documents.txt"),
#         output_path=Path(
#             global_options.DATA_FOLDER, "processed", "trigram", "documents.txt"
#         ),
#         model_path=Path(global_options.MODEL_FOLDER, "phrases", "trigram.mod"),
#         scoring="original_scorer",
#         threshold=global_options.PHRASE_THRESHOLD,
#     )

#     # # train the word2vec model
#     # logging.info(datetime.datetime.now())
#     # logging.info("Training w2v model...")
#     # culture_models.train_w2v_model(
#     #     input_path=Path(
#     #         global_options.DATA_FOLDER, "processed", "trigram", "documents.txt"
#     #     ),
#     #     model_path=Path(global_options.MODEL_FOLDER, "w2v", "w2v.mod"),
#     #     vector_size=global_options.W2V_DIM,
#     #     window=global_options.W2V_WINDOW,
#     #     workers=global_options.N_CORES,
#     #     epochs=global_options.W2V_ITER,
#     # )
#     # logging.info(datetime.datetime.now())
#     # logging.info("Training w2v model end")




# if __name__ == '__main__':
#     #input_base = Path(global_options.DATA_FOLDER, "input_21Aug")
#     #input_base = Path(r"J:\Saeed Work\Speaker_EC_Project\processed")
#     #input_base = Path(r"J:\Saeed Work\Speaker_EC_Project\data")
#     input_base = Path(r"J:\Saeed Work\Speaker_EC_Project\data\USA\CFO")

    


#     #all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]#[10:]
#     all_countries = ['USA']

#     for country in all_countries:
#         global_options.set_country_paths(country)

#         # parsed_doc_path = global_options.PROCESSED_FOLDER / "parsed" / "documents.txt"
#         # unigram_path = global_options.PROCESSED_FOLDER / "unigram" / "documents.txt"
#         # bigram_path = global_options.PROCESSED_FOLDER / "bigram" / "documents.txt"
#         # trigram_path = global_options.PROCESSED_FOLDER / "trigram" / "documents.txt"
#         # w2v_path = global_options.MODEL_FOLDER / "w2v" / "w2v.mod"

#         # parsed_doc_path = r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\parsed\documents.txt"
#         # unigram_path = r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\unigram\documents.txt"
#         # bigram_path = r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\bigram\documents.txt"
#         # trigram_path = r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\trigram\documents.txt"
        

#         parsed_doc_path = Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\parsed\documents.txt")
#         unigram_path    = Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\unigram\documents.txt")
#         bigram_path     = Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\bigram\documents.txt")
#         trigram_path    = Path(r"J:\Saeed Work\Speaker_EC_Project\processed\CFO\USA\trigram\documents.txt")
#         w2v_path = global_options.MODEL_FOLDER / "w2v" / "w2v.mod"

#         if not parsed_doc_path.exists():
#             logging.info(f"⏭️ Skipping {country} (no parsed documents)")
#             continue

#         logging.info(f"\n🔄 Processing {country}")

#         # ✅ Step 1: Clean parsed → unigram
#         if not unigram_path.exists():
#             logging.info(f"  ➤ Cleaning parsed → unigram")
#             clean_file(parsed_doc_path, unigram_path)
#         else:
#             logging.info(f"  ✅ Unigram already exists, skipping")

#         # # ✅ Step 2: Apply bigram model
#         # if not bigram_path.exists():
#         #     logging.info(f"  ➤ Training/applying bigram model")
#         #     culture_models.train_bigram_model(
#         #         input_path=unigram_path,
#         #         model_path=global_options.MODEL_FOLDER / "phrases" / "bigram.mod",
#         #     )
#         #     culture_models.file_bigramer(
#         #         input_path=unigram_path,
#         #         output_path=bigram_path,
#         #         model_path=global_options.MODEL_FOLDER / "phrases" / "bigram.mod",
#         #         scoring="original_scorer",
#         #         threshold=global_options.PHRASE_THRESHOLD,
#         #     )
#         # else:
#         #     logging.info(f"  ✅ Bigram already exists, skipping")

#         # # ✅ Step 3: Apply trigram model
#         # if not trigram_path.exists():
#         #     logging.info(f"  ➤ Training/applying trigram model")
#         #     culture_models.train_bigram_model(
#         #         input_path=bigram_path,
#         #         model_path=global_options.MODEL_FOLDER / "phrases" / "trigram.mod",
#         #     )
#         #     culture_models.file_bigramer(
#         #         input_path=bigram_path,
#         #         output_path=trigram_path,
#         #         model_path=global_options.MODEL_FOLDER / "phrases" / "trigram.mod",
#         #         scoring="original_scorer",
#         #         threshold=global_options.PHRASE_THRESHOLD,
#         #     )
#         # else:
#         #     logging.info(f"  ✅ Trigram already exists, skipping")

#         # # ✅ Step 4: Train Word2Vec model
#         # if not w2v_path.exists():
#         #     logging.info(f"  ➤ Training Word2Vec model")
#         #     culture_models.train_w2v_model(
#         #         input_path=trigram_path,
#         #         model_path=w2v_path,
#         #         vector_size=global_options.W2V_DIM,
#         #         window=global_options.W2V_WINDOW,
#         #         workers=global_options.N_CORES,
#         #         epochs=global_options.W2V_ITER,
#         #     )
#         # else:
#         #     logging.info(f"  ✅ Word2Vec already exists, skipping")



def clean_file_with_ids(in_text_file, in_id_file, out_text_file, out_id_file):
    """
    Clean sentences AND keep sentence IDs aligned.
    Drops both sentence and ID if cleaned sentence is empty.
    """

    a_text_cleaner = preprocess.text_cleaner()

    in_text_file = Path(in_text_file)
    in_id_file = Path(in_id_file)
    out_text_file = Path(out_text_file)
    out_id_file = Path(out_id_file)

    out_text_file.parent.mkdir(parents=True, exist_ok=True)
    out_id_file.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    n_total = 0
    n_kept = 0

    with open(in_text_file, encoding="utf-8", errors="ignore") as f_text, \
         open(in_id_file, encoding="utf-8", errors="ignore") as f_ids, \
         open(out_text_file, "w", encoding="utf-8") as f_out_text, \
         open(out_id_file, "w", encoding="utf-8") as f_out_ids:

        for text, sid in zip(f_text, f_ids):
            n_total += 1

            cleaned_text, _ = a_text_cleaner.clean(text, "0")

            if not cleaned_text:
                continue

            f_out_text.write(cleaned_text + "\n")
            f_out_ids.write(sid.strip() + "\n")
            n_kept += 1

            if n_total % 500_000 == 0:
                logging.info(
                    f"🧹 Processed {n_total:,} sentences | kept {n_kept:,}"
                )

    logging.info(
        f"Finished cleaning. "
        f"Total: {n_total:,} | Kept: {n_kept:,} | "
        f"Time: {(time.time() - start_time)/60:.2f} minutes"
    )


if __name__ == "__main__":

    RERUN_USA_BETA = True # re-run USA

    processed_base = Path(r"J:\Saeed Work\Speaker_EC_Project\processed")

    # all countries EXCEPT USA
    all_countries = [
        c.name for c in (processed_base / CATEGORIES[0]).iterdir()
        if c.is_dir() and c.name != "USA"
    ]

    #all_countries = ["USA"]

    logging.info(f"Countries (excluding USA): {all_countries}")
    logging.info(f"Categories: {CATEGORIES}")

    # for category in CATEGORIES:
    #     for country in all_countries:
    #         print('double_checking', category, country)
    #         parsed_doc_path = (
    #             processed_base / category / country / "parsed" / "documents.txt"
    #         )
    #         unigram_path = (
    #             processed_base / category / country / "unigram" / "documents.txt"
    #         )
    #         bigram_path = (
    #             processed_base / category / country / "bigram" / "documents.txt"
    #         )




    for category in CATEGORIES:
        for country in all_countries:
            print("double_checking", category, country)

            # input country is always the real country
            input_country = country

            # output country may differ (USA → USA_beta)
            if RERUN_USA_BETA and country == "USA":
                output_country = "USA_beta"
            else:
                output_country = country

            parsed_doc_path = (
                processed_base / category / input_country / "parsed" / "documents.txt"
            )

            unigram_path = (
                processed_base / category / output_country / "unigram" / "documents.txt"
            )

            bigram_path = (
                processed_base / category / output_country / "bigram" / "documents.txt"
            )


            if not parsed_doc_path.exists():
                logging.info(f"⏭️ Skipping {category}-{country}: no parsed file")
                continue

            logging.info(f"\n🔄 Processing {category} | {country}")

            # Step 1: Clean
            if not unigram_path.exists():
                logging.info("  ➤ Cleaning parsed → unigram")
                #clean_file(parsed_doc_path, unigram_path)
                clean_file_with_ids(
                    in_text_file=processed_base / category / input_country / "parsed" / "documents.txt",
                    in_id_file=processed_base / category / input_country / "parsed" / "document_sent_ids.txt",
                    out_text_file=processed_base / category / output_country / "unigram" / "documents.txt",
                    out_id_file=processed_base / category / output_country / "unigram" / "document_sent_ids.txt",
                )

            else:
                logging.info("  ✅ Unigram exists, skipping")

            # Step 2: Bigram
            if not bigram_path.exists():
                logging.info("  ➤ Applying bigram model")
                # culture_models.file_bigramer(
                #     input_path=unigram_path,
                #     output_path=bigram_path,
                #     #model_path=global_options.MODEL_FOLDER / "phrases" / "bigram.mod",
                #     model_path = Path(global_options.MODEL_FOLDER) / "phrases" / "bigram.mod",

                #     scoring="original_scorer",
                #     threshold=global_options.PHRASE_THRESHOLD,
                # )

                try:
                    culture_models.file_bigramer(
                        input_path=unigram_path,
                        output_path=bigram_path,
                        model_path = Path(global_options.MODEL_FOLDER) / "phrases" / "bigram.mod",
                        scoring="original_scorer",
                        threshold=global_options.PHRASE_THRESHOLD,
                    )

                except AssertionError:
                    logging.warning(
                        f"⚠️ Bigram assertion failed for {category}-{country}. "
                        "Using unigram fallback."
                    )

                    # record fallback country
                    with open(FALLBACK_LOG, "a", encoding="utf-8") as f:
                        f.write(f"{category},{country},bigram_assertion\n")

                    # fallback: copy unigram → bigram
                    bigram_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(unigram_path, encoding="utf-8") as fin, \
                        open(bigram_path, "w", encoding="utf-8") as fout:
                        for line in fin:
                            fout.write(line)

            else:
                logging.info("  ✅ Bigram exists, skipping")



