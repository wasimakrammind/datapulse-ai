"""Run the pipeline once if the database is empty or missing."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.settings import DB_PATH


def ensure_data_ready():
    needs_init = not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) < 1024

    if needs_init:
        print("[INIT] Database not found or empty — running pipeline...")
        from src.etl.database import init_database, create_schema
        from src.etl.generate_data import generate_all
        from src.etl.pipeline import run_full_pipeline
        from src.data_quality.dq_engine import run_all_dq_checks
        from src.genai.root_cause_agent import analyze_failure

        init_database()
        create_schema()
        generate_all()
        batch_id, status, results = run_full_pipeline()
        run_all_dq_checks(batch_id)
        analyze_failure(batch_id=batch_id)
        print("[INIT] Pipeline complete — data ready.")
    else:
        print("[INIT] Database exists — skipping pipeline.")
