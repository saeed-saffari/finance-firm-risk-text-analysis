
from pathlib import Path
from culture import culture_models
import global_options
import logging

bigram_model_path = Path(global_options.MODEL_FOLDER) / "phrases" / "bigram.mod"
bigram_model_path.parent.mkdir(parents=True, exist_ok=True)

usa_unigram_path = Path(
    r"J:\Saeed Work\Speaker_EC_Project\processed\CFO_CEO\USA\unigram\documents.txt"
)

if not bigram_model_path.exists():
    logging.info("Training bigram model using USA corpus (one time)")

    try:
        culture_models.train_bigram_model(
            input_path=usa_unigram_path,
            model_path=bigram_model_path,
        )
        logging.info("Bigram model training completed successfully.")

    except Exception as e:
        logging.warning(
            f"⚠️ Bigram training raised an error: {e}. "
            "If bigram.mod exists, it will be reused."
        )

else:
    logging.info("Bigram model already exists. Skipping training.")
