"""
Speaker parsing with multiprocessing.

Key assumptions:
- preprocess_parallel.process_document returns (List[str], List[str])
- Long documents are skipped in preprocessing
- Speed and stability are prioritized
"""

import itertools
import os
import time
import logging
from multiprocessing import Pool
from pathlib import Path

from stanza.server import CoreNLPClient

import global_options
from culture import file_util, preprocess_parallel


# ============================================================
# LOGGING
# ============================================================

log_file = r"J:\Saeed Work\Speaker_EC_Project\parse_parallel_speaker_log_jan7_CFO_CEO.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


# ============================================================
# PATHS
# ============================================================

DATA_BASE = Path(global_options.DATA_FOLDER)
PROCESSED_BASE = Path(r"J:\Saeed Work\Speaker_EC_Project\processed")

# For now: run one category only (safe)
CATEGORIES = ["CEO"]



# ============================================================
# CORE PROCESSOR
# ============================================================

def process_largefile(
    input_file,
    output_file,
    input_file_ids,
    output_index_file,
    function_name,
    chunk_size=50,
    start_index=None,
):
    """
    Speaker-safe large file processor.

    Expects function_name(doc, doc_id) -> (List[str], List[str])
    """

    assert file_util.line_counter(input_file) == len(input_file_ids), \
        "Input file and ID file must have same number of rows."

    #output_file.parent.mkdir(parents=True, exist_ok=True)
    #output_index_file.parent.mkdir(parents=True, exist_ok=True)

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_index_file is not None:
        output_index_file = Path(output_index_file)
        output_index_file.parent.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------
    # Resume logic
    # --------------------------------------------------------
    if start_index is None:
        if output_index_file.exists():
            start_index = file_util.line_counter(output_index_file)
        else:
            start_index = 0

    logging.info(f"Starting from document index: {start_index}")

    with open(input_file, encoding="utf-8", errors="ignore") as f_in, \
         open(output_file, "a", encoding="utf-8", buffering=1) as f_out, \
         open(output_index_file, "a", encoding="utf-8", buffering=1) as f_idx:

        # Skip already processed docs
        for _ in range(start_index):
            next(f_in)

        input_file_ids = input_file_ids[start_index:]
        processed_docs = start_index
        start_time = time.time()

        # ----------------------------------------------------
        # ONE Pool for entire run
        # ----------------------------------------------------
        with Pool(global_options.N_CORES) as pool:

            for lines_chunk, ids_chunk in zip(
                itertools.zip_longest(*[f_in] * chunk_size),
                itertools.zip_longest(*[iter(input_file_ids)] * chunk_size),
            ):
                lines_chunk = [l for l in lines_chunk if l is not None]
                ids_chunk = [i for i in ids_chunk if i is not None]

                if not lines_chunk:
                    break

                try:
                    results = pool.starmap(
                        function_name,
                        zip(lines_chunk, ids_chunk)
                    )
                except Exception as e:
                    logging.error(
                        f"CoreNLP failure near doc {processed_docs}: {e}"
                    )
                    raise

                # ------------------------------------------------
                # WRITE SENTENCE BY SENTENCE (CRITICAL)
                # ------------------------------------------------
                for (sentences, sent_ids), doc_id in zip(results, ids_chunk):

                    if not sentences or not sent_ids:
                        logging.warning(f"Skipped empty output for doc_id={doc_id}")
                        continue

                    for s, sid in zip(sentences, sent_ids):
                        f_out.write(s + "\n")
                        f_idx.write(sid + "\n")

                processed_docs += len(lines_chunk)

                # ------------------------------------------------
                # Progress logging
                # ------------------------------------------------
                if processed_docs % 500 == 0:
                    elapsed = (time.time() - start_time) / 60
                    logging.info(
                        f"📊 Processed {processed_docs} docs | "
                        f"⏱ {elapsed:.2f} min"
                    )


# ============================================================
# COUNTRY + CATEGORY DRIVER
# ============================================================

def process_country_category(country, category):
    logging.info(f"\n===== Processing {country} | {category} =====")

    input_dir = DATA_BASE / country / category
    input_file = input_dir / "documents.txt"
    input_ids_file = input_dir / "document_id.txt"

    if not input_file.exists() or not input_ids_file.exists():
        logging.info(f"Skipping {country}-{category}: missing input files")
        return

    input_file_ids = file_util.file_to_list(input_ids_file)

    parsed_dir = PROCESSED_BASE / category / country / "parsed"
    parsed_dir.mkdir(parents=True, exist_ok=True)

    output_file = parsed_dir / "documents.txt"
    output_index_file = parsed_dir / "document_sent_ids.txt"

    logging.info(f"Starting parsing for {country}-{category}")

    process_largefile(
        input_file=input_file,
        output_file=output_file,
        input_file_ids=input_file_ids,
        output_index_file=output_index_file,
        function_name=preprocess_parallel.process_document,
        chunk_size=global_options.PARSE_CHUNK_SIZE,
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    #all_countries = ["USA"]
    all_countries = ["VEN", "VGB", "VIR", "VNM", "ZAF"]
    logging.info(f"Countries: {all_countries}")
    logging.info(f"Categories: {CATEGORIES}")

    os.environ["CORENLP_HOME"] = (
        global_options.CORENLP_HOME
        if hasattr(global_options, "CORENLP_HOME")
        else ""
    )

    with CoreNLPClient(
        properties={
            "ner.applyFineGrained": "false",
            # IMPORTANT: no depparse for speaker project
            "annotators": "tokenize, ssplit, pos, lemma, ner",
        },
        classpath=r"J:\Saeed Work\May 20\stanford-corenlp-full-2018-10-05\*",
        memory=global_options.RAM_CORENLP,
        threads=global_options.N_CORES,
        timeout=12000000,
        endpoint="http://localhost:9001",
        start_server=True,
        be_quiet=True,
    ):

        for country in all_countries:
            for category in CATEGORIES:
                process_country_category(country, category)

        logging.info("All parsing completed successfully.")
