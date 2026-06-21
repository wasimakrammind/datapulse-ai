"""Master script — runs the full DataPulse AI pipeline end-to-end."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.etl.database import init_database, create_schema
from src.etl.generate_data import generate_all
from src.etl.pipeline import run_full_pipeline
from src.data_quality.dq_engine import run_all_dq_checks
from src.genai.root_cause_agent import analyze_failure


def main():
    print("\n" + "="*70)
    print("  DataPulse AI — Full Pipeline Execution")
    print("="*70)

    # Step 1: Initialize database
    print("\n[STEP 1] Initializing database...")
    init_database()
    create_schema()

    # Step 2: Generate synthetic data
    print("\n[STEP 2] Generating synthetic datasets...")
    generate_all()

    # Step 3: Run ETL pipeline
    print("\n[STEP 3] Running ETL pipeline...")
    batch_id, status, results = run_full_pipeline()

    # Step 4: Run data quality checks
    print("\n[STEP 4] Running data quality checks...")
    dq_summary = run_all_dq_checks(batch_id)

    # Step 5: GenAI root-cause analysis
    print("\n[STEP 5] Running GenAI root-cause analysis...")
    analysis = analyze_failure(batch_id=batch_id)
    print("\n" + "-"*60)
    print("ROOT-CAUSE ANALYSIS:")
    print("-"*60)
    print(analysis)

    # Summary
    print("\n" + "="*70)
    print("  PIPELINE COMPLETE")
    print("="*70)
    print(f"  Batch ID:           {batch_id}")
    print(f"  Pipeline Status:    {status}")
    print(f"  DQ Checks Passed:   {dq_summary['passed']}/{dq_summary['total_checks']}")
    print(f"  DQ Pass Rate:       {dq_summary['pass_rate']}%")
    print(f"  Critical Failures:  {dq_summary['critical_failures']}")
    print("="*70)
    print("\n  Next steps:")
    print("    - API:       uvicorn src.api.app_with_genai:app --reload")
    print("    - Dashboard: streamlit run src/dashboard/app.py")
    print("    - Tests:     pytest tests/ -v")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
