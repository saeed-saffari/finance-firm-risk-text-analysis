# Finance EC Text Analysis Project

This repository contains the full workflow for processing Earnings Call (EC) transcripts, extracting text features, building domain dictionaries, and generating firm-level scores.  
We do not include real data. Only small synthetic samples and examples are provided.

The goal of this project is to take raw EC transcripts from many countries and produce structured, comparable text-based indicators, such as:

- TF and TF-IDF scores  
- Word2Vec-based dictionary expansions  
- Firm-level supply, demand, and domain risk scores  
- Combined risk scores based on keyword windows  
- Cleaned unigram, bigram, and trigram corpora  

---

## Project Structure

```
finance-firm-risk-text-analysis/
│
├── codes/                     # All Python scripts
├── inputs_sample/             # Sample inputs showing expected structure
├── outputs_sample/            # Sample outputs demonstrating final format
├── diagrams                   # Pipeline overview diagram
└── README.md
```

---

## Folder Descriptions

### inputs_sample/
Shows small, synthetic examples of the expected input format.

### outputs_sample/
Demonstrates the final output files created by the pipeline.

---

## Code Overview

### 1. EC_Extraction_May17_Saeed.py
Extracts Q&A sections from raw EC files.

### 2. preprocess.py
Cleans parsed sentences and produces unigram text.

### 3. parse_parallel.py
Splits raw transcripts into sentences using CoreNLP.

### 4. clean_and_train.py
Trains bigram, trigram, and Word2Vec models. Applies phrase models.

### 5. culture_dictionary.py
Expands seed words into domain dictionaries using Word2Vec.

### 6. culture_models.py
Implements phrase model training and Word2Vec training.

### 7. score.py
Creates TF, TF-IDF, and WF-IDF document-level scores.

### 8. aggregate_scores.py
Aggregates document scores to firm-level scores.

### 9. merge_scores.py
Merges firm-level and document-level score files.

### 10. risk_combination_score.py
Calculates risk-based combination scores using dictionary and risk windows.

---

## Pipeline Summary

1. Extract Q&A  
2. Parse into sentences  
3. Clean text  
4. Train phrase and Word2Vec models  
5. Expand dictionaries  
6. Score documents  
7. Aggregate to firm-level  
8. Compute risk combination scores  

---

## Notes

- Real datasets are not included.  
- The pipeline is modular and reproducible.  
- Sample folders help demonstrate the expected input/output structure.  
