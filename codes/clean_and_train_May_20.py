import datetime
import functools
import logging
import sys
from pathlib import Path

import pandas as pd
import time


import global_options
#import parse_parallel
import parse_parallel_May20 as parse                        # saeed edit May 20
from culture import culture_models, file_util, preprocess


#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log_file = "J:\Saeed Work\May 20\clean_and_train_log3_aug21.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def clean_file(in_file, out_file):
    """Clean the entire corpus (output from CoreNLP)

    Args:
        in_file (str or Path): Input corpus, each line is a sentence
        out_file (str or Path): Output corpus
    """
    a_text_cleaner = preprocess.text_cleaner()

    start_time = time.time()
    parse.process_largefile(
        input_file=in_file,
        output_file=out_file,
        input_file_ids=[
            str(i) for i in range(file_util.line_counter(in_file))
        ],  # fake IDs (do not need IDs for this function).
        output_index_file=None,
        function_name=functools.partial(a_text_cleaner.clean),
        chunk_size=200000,
        #encoding="utf-8",  # Ensure UTF-8 encoding for output
    )
    end_time = time.time()
    logging.info(f"Finished cleaning: {datetime.datetime.now()}")
    logging.info(f"Total time: {(end_time - start_time)/60:.4f} minutes")


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




if __name__ == '__main__':
    input_base = Path(global_options.DATA_FOLDER, "input_21Aug")
    all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]#[10:]

    for country in all_countries:
        global_options.set_country_paths(country)

        parsed_doc_path = global_options.PROCESSED_FOLDER / "parsed" / "documents.txt"
        unigram_path = global_options.PROCESSED_FOLDER / "unigram" / "documents.txt"
        bigram_path = global_options.PROCESSED_FOLDER / "bigram" / "documents.txt"
        trigram_path = global_options.PROCESSED_FOLDER / "trigram" / "documents.txt"
        w2v_path = global_options.MODEL_FOLDER / "w2v" / "w2v.mod"

        if not parsed_doc_path.exists():
            logging.info(f"⏭️ Skipping {country} (no parsed documents)")
            continue

        logging.info(f"\n🔄 Processing {country}")

        # ✅ Step 1: Clean parsed → unigram
        if not unigram_path.exists():
            logging.info(f"  ➤ Cleaning parsed → unigram")
            clean_file(parsed_doc_path, unigram_path)
        else:
            logging.info(f"  ✅ Unigram already exists, skipping")

        # ✅ Step 2: Apply bigram model
        if not bigram_path.exists():
            logging.info(f"  ➤ Training/applying bigram model")
            culture_models.train_bigram_model(
                input_path=unigram_path,
                model_path=global_options.MODEL_FOLDER / "phrases" / "bigram.mod",
            )
            culture_models.file_bigramer(
                input_path=unigram_path,
                output_path=bigram_path,
                model_path=global_options.MODEL_FOLDER / "phrases" / "bigram.mod",
                scoring="original_scorer",
                threshold=global_options.PHRASE_THRESHOLD,
            )
        else:
            logging.info(f"  ✅ Bigram already exists, skipping")

        # ✅ Step 3: Apply trigram model
        if not trigram_path.exists():
            logging.info(f"  ➤ Training/applying trigram model")
            culture_models.train_bigram_model(
                input_path=bigram_path,
                model_path=global_options.MODEL_FOLDER / "phrases" / "trigram.mod",
            )
            culture_models.file_bigramer(
                input_path=bigram_path,
                output_path=trigram_path,
                model_path=global_options.MODEL_FOLDER / "phrases" / "trigram.mod",
                scoring="original_scorer",
                threshold=global_options.PHRASE_THRESHOLD,
            )
        else:
            logging.info(f"  ✅ Trigram already exists, skipping")

        # ✅ Step 4: Train Word2Vec model
        if not w2v_path.exists():
            logging.info(f"  ➤ Training Word2Vec model")
            culture_models.train_w2v_model(
                input_path=trigram_path,
                model_path=w2v_path,
                vector_size=global_options.W2V_DIM,
                window=global_options.W2V_WINDOW,
                workers=global_options.N_CORES,
                epochs=global_options.W2V_ITER,
            )
        else:
            logging.info(f"  ✅ Word2Vec already exists, skipping")















