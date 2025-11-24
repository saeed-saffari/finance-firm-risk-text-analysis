"""Global options for analysis
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Set

# Hardware options
N_CORES: int = 6  # max number of CPU cores to use
RAM_CORENLP: str = "8G"  # max RAM allocated for parsing using CoreNLP; increase to speed up parsing
PARSE_CHUNK_SIZE: int = 100 # number of lines in the input file to process using CoreNLP at once. Increase on workstations with larger RAM (e.g. to 1000 if RAM is 64G)

# Directory locations
os.environ["CORENLP_HOME"] = r"J:\Saeed Work\May 20\stanford-corenlp-full-2018-10-05"  # location of the CoreNLP models
DATA_FOLDER: str = r"J:\Saeed Work\May 20\data"
MODEL_FOLDER: str = r"J:\Saeed Work\May 20\models" # will be created if does not exist
#OUTPUT_FOLDER: str = r"J:\Saeed Work\May 20\outputs" # will be created if does not exist; !!! WARNING: existing files will be removed !!!
OUTPUT_FOLDER: str = r"J:\Saeed Work\May 20\output_21Aug" # will be created if does not exist; !!! WARNING: existing files will be removed !!!

# New August 21 batch locations
INPUT_FOLDER_AUG21: Path = Path(DATA_FOLDER, "input_21Aug")
PROCESSED_FOLDER_AUG21: Path = Path(DATA_FOLDER, "processed_21Aug")

# Parsing and analysis options
STOPWORDS: Set[str] = set(
    Path("J:\Saeed Work\May 20", "StopWords_Generic.txt").read_text().lower().split()
)  # Set of stopwords from https://sraf.nd.edu/textual-analysis/resources/#StopWords




PHRASE_THRESHOLD: int = 10  # threshold of the phraser module (smaller -> more phrases)
PHRASE_MIN_COUNT: int = 10  # min number of times a bigram needs to appear in the corpus to be considered as a phrase
W2V_DIM: int = 300  # dimension of word2vec vectors
W2V_WINDOW: int = 5  # window size in word2vec
W2V_ITER: int = 20  # number of iterations in word2vec
N_WORDS_DIM: int = 500  # max number of words in each dimension of the dictionary
DICT_RESTRICT_VOCAB = None # change to a fraction number (e.g. 0.2) to restrict the dictionary vocab in the top 20% of most frequent vocab

# Inputs for constructing the expanded dictionary
# DIMS: List[str] = [#"integrity", 
#                    #"teamwork", 
#                    "innovation", "respect", "quality", "supply"]

# DIMS: List[str] =["compensation_and_benefits",	
#                     "dei",	'demographics_and_others', 	
#                     'health_and_safety_general',
#                     'labor_relations_and_culture']

#DIMS: List[str] =["Diversity"]
#DIMS: List[str] =["sexual_harassment"]
#DIMS: List[str] =["toxicity"]
DIMS: List[str] =["DEI_JAR"]
#DIMS: List[str] =["DEI_GD"]
#DIMS: List[str] =["DEI_Comprehensive"]
#DIMS: List[str] =["DEI_black"]







SEED_WORDS: Dict[str, List[str]] = {
    # "integrity": [
    #     "integrity",
    #     "ethic",
    #     "ethical",
    #     "accountable",
    #     "accountability",
    #     "trust",
    #     "honesty",
    #     "honest",
    #     "honestly",
    #     "fairness",
    #     "responsibility",
    #     "responsible",
    #     "transparency",
    #     "transparent",
    # ],
    # "teamwork": [
    #     "teamwork",
    #     "collaboration",
    #     "collaborate",
    #     "collaborative",
    #     "cooperation",
    #     "cooperate",
    #     "cooperative",
    # ],
    "innovation": [
        "innovation",
        "innovate",
        "innovative",
        "creativity",
        "creative",
        "create",
        "passion",
        "passionate",
        "efficiency",
        "efficient",
        "excellence",
        "pride",
    ],
    "respect": [
        "respectful",
        "talent",
        "talented",
        "employee",
        "dignity",
        "empowerment",
        "empower",
    ],
    "quality": [
        "quality",
        "customer",
        "customer_commitment",
        "dedication",
        "dedicated",
        "dedicate",
        "customer_expectation",
    ],
    "supply": [
        "supply", "supplier", 
        "suppliers", "component", 
        "inventory", "logistics", 
        "procurement", "shortage",
        "supply_chain", "supply_disruption", 
        "raw_material", "cost_increase", 
        "bottleneck", "delay", 
        "sourcing_risk", "distribution",
        "transportation", "manufacturing_delay", 
        "shipment", "lead_time", "order_backlog",
        "inventory", "manufacturing"
    ]
}


# # Create directories if not exist
# Path(DATA_FOLDER, "processed", "parsed").mkdir(parents=True, exist_ok=True)
# Path(DATA_FOLDER, "processed", "unigram").mkdir(parents=True, exist_ok=True)
# Path(DATA_FOLDER, "processed", "bigram").mkdir(parents=True, exist_ok=True)
# Path(DATA_FOLDER, "processed", "trigram").mkdir(parents=True, exist_ok=True)
# Path(MODEL_FOLDER, "phrases").mkdir(parents=True, exist_ok=True)
# Path(MODEL_FOLDER, "w2v").mkdir(parents=True, exist_ok=True)
# Path(OUTPUT_FOLDER, "dict").mkdir(parents=True, exist_ok=True)
# Path(OUTPUT_FOLDER, "scores").mkdir(parents=True, exist_ok=True)
# Path(OUTPUT_FOLDER, "scores", "temp").mkdir(parents=True, exist_ok=True)
# Path(OUTPUT_FOLDER, "scores", "word_contributions").mkdir(parents=True, exist_ok=True)

def set_country_paths(country_name: str):
    """
    Dynamically set folders for a given country.
    Call this at the beginning of each loop in your script.
    """
    global MODEL_FOLDER, OUTPUT_FOLDER, PROCESSED_FOLDER

    MODEL_FOLDER = Path(DATA_FOLDER, "models", country_name)
    #OUTPUT_FOLDER = Path(DATA_FOLDER, "outputs", country_name)
    OUTPUT_FOLDER = Path(DATA_FOLDER, "output_21Aug", country_name)
    #PROCESSED_FOLDER = Path(DATA_FOLDER, "processed", country_name)
    PROCESSED_FOLDER = Path(DATA_FOLDER, "processed_new_files_Aug21", country_name)     # edit new file

    # Create all folders if needed
    for folder in [
        PROCESSED_FOLDER / "parsed",
        PROCESSED_FOLDER / "unigram",
        PROCESSED_FOLDER / "bigram",
        PROCESSED_FOLDER / "trigram",
        MODEL_FOLDER / "phrases",
        MODEL_FOLDER / "w2v",
        OUTPUT_FOLDER / "dict",
        OUTPUT_FOLDER / "scores",
        OUTPUT_FOLDER / "scores" / "temp",
        OUTPUT_FOLDER / "scores" / "word_contributions",
    ]:
        folder.mkdir(parents=True, exist_ok=True)
