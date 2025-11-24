"""Implementation of parse.py that supports multiprocess
Main differences are 1) using Pool.starmap in process_largefile and 2) attach to local CoreNLP server in process_largefile.process_document
"""
import datetime
import itertools
import os
from multiprocessing import Pool
from pathlib import Path
import time
import logging

from stanza.server import CoreNLPClient
#from stanza.server import CoreNLPClient#, StartServer

#from stanfordnlp.server import CoreNLPClient

import global_options
from culture import file_util, preprocess_parallel

log_file = "J:\Saeed Work\May 20\parse_parallel_log_Aug21.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
input_base = Path(global_options.DATA_FOLDER, "input_21Aug")
all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]



def process_largefile(
    input_file,
    output_file,
    input_file_ids,
    output_index_file,
    function_name,
    chunk_size=100,
    start_index=None,
):
    """ A helper function that transforms an input file + a list of IDs of each line (documents + document_IDs) to two output files (processed documents + processed document IDs) by calling function_name on chunks of the input files. Each document can be decomposed into multiple processed documents (e.g. sentences). 
    Supports parallel with Pool.

    Arguments:
        input_file {str or Path} -- path to a text file, each line is a document
        ouput_file {str or Path} -- processed linesentence file (remove if exists)
        input_file_ids {str]} -- a list of input line ids
        output_index_file {str or Path} -- path to the index file of the output
        function_name {callable} -- A function that processes a list of strings, list of ids and return a list of processed strings and ids.
        chunk_size {int} -- number of lines to process each time, increasing the default may increase performance
        start_index {int} -- line number to start from (index starts with 0)

    Writes:
        Write the ouput_file and output_index_file
    """
    try:
        if start_index is None:
            # if start from the first line, remove existing output file
            # else append to existing output file
            os.remove(str(output_file))
            os.remove(str(output_index_file))
    except OSError:
        pass
    assert file_util.line_counter(input_file) == len(
        input_file_ids
    ), "Make sure the input file has the same number of rows as the input ID file. "

    with open(input_file, newline="\n", encoding="utf-8", errors="ignore") as f_in:
        line_i = 0
        # jump to index
        if start_index is not None:
            # start at start_index line
            for _ in range(start_index):
                next(f_in)
            input_file_ids = input_file_ids[start_index:]
            line_i = start_index
        
        start_time = time.time()
        for next_n_lines, next_n_line_ids in zip(
            itertools.zip_longest(*[f_in] * chunk_size),
            itertools.zip_longest(*[iter(input_file_ids)] * chunk_size),
        ):
            line_i += chunk_size
            if line_i % 1000 == 0 or line_i == chunk_size:
                elapsed = time.time() - start_time
                #logging.info(f"[{datetime.datetime.now()}] 📊 Processed: {line_i} docs | ⏱ Elapsed: {elapsed / 60:.2f} min")
                logging.info(f"📊 Processed: {line_i} docs | ⏱ Elapsed: {elapsed / 60:.2f} min")
            #print(datetime.datetime.now())
            logging.info(f"Processing line: {line_i}.")
            #print(f"Processing line: {line_i}.")
            
            next_n_lines = [line for line in next_n_lines if line is not None]
            next_n_line_ids = [line_id for line_id in next_n_line_ids if line_id is not None]
            output_lines = []
            output_line_ids = []

            # with Pool(global_options.N_CORES) as pool:
            with Pool(global_options.N_CORES) as pool:
                results = pool.starmap(function_name, zip(next_n_lines, next_n_line_ids))

            for (output_line, output_line_id), doc_id in zip(results, next_n_line_ids):
                if output_line.strip() == "":
                    logging.warning(f"⚠️ Skipped document (empty output): {doc_id}")
                    with open("skipped_ids.txt", "a", encoding="utf-8") as f:
                        f.write(f"{doc_id}\n")
                    #continue
                output_lines.append(output_line)
                output_line_ids.append(output_line_id)


                
                # for output_line, output_line_id in pool.starmap(
                #     function_name, zip(next_n_lines, next_n_line_ids)
                # ):
                #     output_lines.append(output_line)
                #     output_line_ids.append(output_line_id)

                
            output_lines = "\n".join(output_lines) + "\n"
            output_line_ids = "\n".join(output_line_ids) + "\n"
            with open(output_file, "a", newline="\n", encoding="utf-8") as f_out:  # Specify encoding
                f_out.write(output_lines)
            if output_index_file is not None:
                with open(output_index_file, "a", newline="\n", encoding="utf-8") as f_out:  # Specify encoding
                    f_out.write(output_line_ids)


if __name__ == "__main__":
    import os

    input_base = Path(global_options.DATA_FOLDER, "input_21Aug")
    all_countries = [f.name for f in input_base.iterdir() if f.is_dir()]#[15:]
    os.environ["CORENLP_HOME"] = r"J:\Saeed Work\May 20\stanford-corenlp-full-2018-10-05"

    with CoreNLPClient(
        properties={
            "ner.applyFineGrained": "false",
            "annotators": "tokenize, ssplit, pos, lemma, ner, depparse",
        },
        #classpath= r"J:\Saeed Work\stanford-corenlp-full-2018-10-05\*", # saeed edit
        classpath="J:\Saeed Work\May 20\stanford-corenlp-full-2018-10-05\*",   # saeed edit
        memory=global_options.RAM_CORENLP,
        threads=global_options.N_CORES,
        timeout=12000000,
        endpoint="http://localhost:9002",  # change port here and in preprocess_parallel.py if 9002 is occupied
        max_char_length=1000000,
        start_server=True,
        be_quiet=True
        
    ) as client:
        # in_file = Path(global_options.DATA_FOLDER, "input", "documents.txt")
        # in_file_index = file_util.file_to_list(
        #     Path(global_options.DATA_FOLDER, "input", "document_ids.txt")
        # )
        # out_file = Path(
        #     global_options.DATA_FOLDER, "processed", "parsed", "documents.txt"
        # )
        # output_index_file = Path(
        #     global_options.DATA_FOLDER, "processed", "parsed", "document_sent_ids.txt"
        # )

        for country in all_countries:
            logging.info(f"\n🌍 Processing country: {country}")
            global_options.set_country_paths(country)

            input_file = input_base / country / "documents.txt"
            input_ids_file = input_base / country / "document_ids.txt"

            if not input_file.exists() or not input_ids_file.exists():
                logging.info(f"⚠️ Skipping {country}: missing input files.")
                continue

            input_file_ids = file_util.file_to_list(input_ids_file)
            output_file = global_options.PROCESSED_FOLDER / "parsed" / "documents.txt"
            output_index_file = global_options.PROCESSED_FOLDER / "parsed" / "document_sent_ids.txt"

            # ✅ SKIP if already processed
            if output_file.exists() and output_index_file.exists():
                print(f"✅ Already processed {country}, skipping.")
                continue


            process_largefile(
                input_file=input_file,
                output_file=output_file,
                input_file_ids=input_file_ids,
                output_index_file=output_index_file,
                function_name=preprocess_parallel.process_document,
                chunk_size=global_options.PARSE_CHUNK_SIZE,
                #start_index=378191,  # <<< Start from here
            )
