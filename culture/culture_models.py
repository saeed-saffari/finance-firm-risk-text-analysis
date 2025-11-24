import sys

sys.path.append("..")
import concurrent.futures
import datetime
from functools import partial
from multiprocessing import Pool, freeze_support
from pathlib import Path

import gensim
import global_options
import tqdm
from gensim import models

from . import file_util


def train_bigram_model(input_path, model_path):
    """ Train a phrase model and save it to the disk. 
    
    Arguments:
        input_path {str or Path} -- input corpus
        model_path {str or Path} -- where to save the trained phrase model?
    
    Returns:
        gensim.models.phrases.Phrases -- the trained phrase model
    """
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    print(datetime.datetime.now())
    print("Training phraser...")
    corpus = gensim.models.word2vec.PathLineSentences(
        str(input_path), max_sentence_length=10000000
    )
    n_lines = file_util.line_counter(input_path)
    bigram_model = models.phrases.Phrases(
        tqdm.tqdm(corpus, total=n_lines),
        min_count=global_options.PHRASE_MIN_COUNT,
        scoring="default",
        threshold=global_options.PHRASE_THRESHOLD,
        #common_terms=global_options.STOPWORDS,
    )
    bigram_model.save(str(model_path))
    return bigram_model


def bigram_transform(line, bigram_phraser):
    """ Helper file for file_bigramer.
    Note: Needs a phraser object or phrase model.

    Arguments:
        line {str}: a line 
        return: a line with phrases joined using "_"
    """
    return " ".join(bigram_phraser[line.split()])


def file_bigramer(input_path, output_path, model_path, threshold=None, scoring=None):
    """ Transform an input text file into a file with 2-word phrases. 
    Apply again to learn 3-word phrases. 

    Arguments:
        input_path {str}: Each line is a sentence
        output_path {str}: Each line is a sentence with 2-word phrases concatenated
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    bigram_model = gensim.models.phrases.Phrases.load(str(model_path))
    if scoring is not None:
        bigram_model.scoring = getattr(gensim.models.phrases, scoring)
    if threshold is not None:
        bigram_model.threshold = threshold

    # Counter for replaced errors
    error_count = 0

    # Read the input file with error handling for problematic characters
    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        input_data = f.readlines()

    # Count replacement errors in input data
    for line in input_data:
        error_count += line.count("�")  # Unicode replacement character

    # Transform each line using the bigram model
    data_bigram = [bigram_transform(l, bigram_model) for l in tqdm.tqdm(input_data)]

    # Write the output file with error handling for problematic characters
    with open(output_path, "w", encoding="utf-8", errors="replace") as f:
        f.write("\n".join(data_bigram) + "\n")

    # Validate the number of lines in the output
    assert len(input_data) == file_util.line_counter(output_path)

    # Log the number of errors replaced
    print(f"Number of replacement errors: {error_count}")


def train_w2v_model(input_path, model_path, *args, **kwargs):
    """ Train a word2vec model using the LineSentence file in input_path, 
    save the model to model_path.
    
    Arguments:
        input_path {str} -- Corpus for training, each line is a sentence
        model_path {str} -- Where to save the model? 
    """
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    corpus_confcall = gensim.models.word2vec.PathLineSentences(
        str(input_path), max_sentence_length=10000000
    )
    model = gensim.models.Word2Vec(corpus_confcall, *args, **kwargs)
    model.save(str(model_path))