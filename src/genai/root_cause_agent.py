"""GenAI Root-Cause Assistant - analyzes failed pipelines and DQ issues (SQLite)."""
import sys, os, json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import OPENAI_API_KEY, LLM_MODEL
from src.etl.database import get_cursor

try:
    from langchain_openai import ChatOpenAI
    from langchain.prompts import ChatPromptTemplate
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


SYSTEM_PROMPT = """You are an expert Data Platform Engineer and Root-Cause Analyst.
You work on the DataPulse AI platform - a big data quality and observability system.

When given pipeline failure logs and data quality results, you must analyze and provide:

1. **What Failed**: Clear description of the failure
2. **Source Identification**: Which data source, table, or column caused the issue
3. **Rule/Check That Failed**: Which specific quality rule or pipeline step failed
4. **Root Cause Analysis**: Most likely reason for the failure
5. **Suggested Fix**: Concrete, actionable remediation steps
6. **Business Impact**: What downstream processes or reports are affected
7. **Recommended Owner**: Which team should own the fix (Data Engineering, Data Ops, Source Team, Platform Team)

Be specific, technical, and actionable. Reference actual table names, column names, and metrics from the data provided."""


def _rows_to_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _gather_failure_context(pipeline_run_id=None, batch_id=None):
    context = {}

    with get_cursor() as (cur, conn):
        if pipeline_run_id:
            cur.execute("SELECT * FROM fact_pipeline_runs WHERE pipeline_run_id = ?", (pipeline_run_id,))
            rows = _rows_to_dicts(cur)
            if rows:
                context['pipeline_run'] = rows[0]
                batch_id = batch_id or rows[0].get('batch_id')

        if batch_id:
            cur.execute("""
                SELECT rule_name, rule_type, table_name, column_name, severity,
                       passed, failed_count, total_count, failure_percentage, details
                FROM fact_data_quality_results
                WHERE batch_id = ? AND passed = 0
                ORDER BY severity DESC, failure_percentage DESC
            """, (batch_id,))
            context['failed_dq_checks'] = _rows_to_dicts(cur)

            cur.execute("""
                SELECT source_table, failure_reason, rule_name, COUNT(*) as count
                FROM quarantine_records WHERE batch_id = ?
                GROUP BY source_table, failure_reason, rule_name
                ORDER BY count DESC
            """, (batch_id,))
            context['quarantine_summary'] = _rows_to_dicts(cur)

            cur.execute("""
                SELECT table_name, operation, source_count, target_count,
                       reconciliation_status, details
                FROM audit_log
                WHERE batch_id = ? AND reconciliation_status = 'mismatch'
            """, (batch_id,))
            context['reconciliation_mismatches'] = _rows_to_dicts(cur)

    return context


def _format_context(context):
    parts = []
    if 'pipeline_run' in context:
        run = context['pipeline_run']
        parts.append(f"""## Pipeline Run
- Run ID: {run.get('pipeline_run_id')}
- Pipeline: {run.get('pipeline_name')}
- Status: {run.get('status')}
- Runtime: {run.get('runtime_seconds')}s
- Input: {run.get('input_count')} | Output: {run.get('output_count')} | Rejected: {run.get('rejected_count')}
- Error: {run.get('error_message') or 'None'}""")

    if context.get('failed_dq_checks'):
        parts.append("\n## Failed Data Quality Checks")
        for c in context['failed_dq_checks']:
            parts.append(f"- [{c['severity'].upper()}] {c['rule_name']} on {c['table_name']}.{c['column_name']}: "
                         f"{c['failed_count']}/{c['total_count']} failed ({c['failure_percentage']}%) - {c['details']}")

    if context.get('quarantine_summary'):
        parts.append("\n## Quarantined Records")
        for q in context['quarantine_summary']:
            parts.append(f"- {q['source_table']}: {q['count']} records quarantined - {q['failure_reason']} (rule: {q['rule_name']})")

    if context.get('reconciliation_mismatches'):
        parts.append("\n## Source-to-Target Mismatches")
        for m in context['reconciliation_mismatches']:
            parts.append(f"- {m['table_name']}: source={m['source_count']}, target={m['target_count']} - {m['details']}")

    return "\n".join(parts)


def analyze_with_langchain(context_text):
    llm = ChatOpenAI(model=LLM_MODEL, temperature=0.2, api_key=OPENAI_API_KEY)
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "Analyze the following pipeline failure and data quality issues:\n\n{context}\n\nProvide a detailed root-cause analysis.")
    ])
    chain = prompt | llm
    response = chain.invoke({"context": context_text})
    return response.content


def analyze_with_openai(context_text):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyze the following pipeline failure and data quality issues:\n\n{context_text}\n\nProvide a detailed root-cause analysis."}
        ],
        temperature=0.2,
        max_tokens=2000
    )
    return response.choices[0].message.content


def analyze_with_rules(context):
    """Rule-based fallback when no LLM API key is configured."""
    findings = []

    if context.get('failed_dq_checks'):
        for check in context['failed_dq_checks']:
            finding = {
                "what_failed": f"Data quality check '{check['rule_name']}' failed on {check['table_name']}",
                "source": f"{check['table_name']}.{check['column_name']}",
                "rule_failed": check['rule_name'],
                "severity": check['severity'],
            }

            rule_type = check.get('rule_type', '')
            if 'null' in rule_type:
                finding["root_cause"] = f"Column '{check['column_name']}' has {check['failed_count']} null/empty values."
                finding["suggested_fix"] = "Validate source data completeness before ingestion. Add NOT NULL constraints or default values."
                finding["business_impact"] = "Downstream reports will have incomplete data, leading to inaccurate metrics."
                finding["recommended_owner"] = "Source Team / Data Engineering"
            elif 'duplicate' in rule_type:
                finding["root_cause"] = f"Found {check['failed_count']} duplicate values in '{check['column_name']}'."
                finding["suggested_fix"] = "Implement idempotent ingestion with UPSERT logic. Add unique constraints."
                finding["business_impact"] = "Duplicate records inflate counts and revenue in reporting."
                finding["recommended_owner"] = "Data Engineering"
            elif 'referential' in rule_type:
                finding["root_cause"] = f"{check['failed_count']} orphan records found."
                finding["suggested_fix"] = "Verify parent records loaded before child records. Add FK validation."
                finding["business_impact"] = "Joins will drop orphan records, causing data loss in curated layer."
                finding["recommended_owner"] = "Data Engineering"
            elif 'negative' in rule_type:
                finding["root_cause"] = f"{check['failed_count']} negative values in '{check['column_name']}'."
                finding["suggested_fix"] = "Add CHECK constraints. Quarantine negative values for manual review."
                finding["business_impact"] = "Negative values distort sum/avg aggregations in financial reports."
                finding["recommended_owner"] = "Source Team"
            elif 'reconciliation' in rule_type:
                finding["root_cause"] = f"Source-to-target mismatch: {check['details']}"
                finding["suggested_fix"] = "Investigate rejected/quarantined records. Verify no data loss."
                finding["business_impact"] = "Data completeness SLA violated."
                finding["recommended_owner"] = "Data Engineering / Data Ops"
            else:
                finding["root_cause"] = f"Quality check failed: {check['details']}"
                finding["suggested_fix"] = "Review the specific rule and failed records."
                finding["business_impact"] = "Data quality degradation in downstream consumers."
                finding["recommended_owner"] = "Data Engineering"

            findings.append(finding)

    if context.get('quarantine_summary'):
        total_quarantined = sum(q['count'] for q in context['quarantine_summary'])
        findings.append({
            "what_failed": f"Total {total_quarantined} records quarantined across sources",
            "source": ", ".join(set(q['source_table'] for q in context['quarantine_summary'])),
            "rule_failed": ", ".join(set(q['rule_name'] for q in context['quarantine_summary'])),
            "root_cause": "Records failed validation rules and were moved to quarantine.",
            "suggested_fix": "Review quarantined records. Fix source data issues. Reprocess after correction.",
            "business_impact": "Quarantined records excluded from curated layer.",
            "recommended_owner": "Data Engineering / Source Team"
        })

    if not findings:
        return "No failures detected. All pipeline runs and data quality checks passed successfully."

    report = "# DataPulse AI - Root-Cause Analysis Report\n\n"
    report += f"**Generated**: {datetime.now().isoformat()}\n\n"

    for i, f in enumerate(findings, 1):
        report += f"## Finding #{i}\n\n"
        for key, val in f.items():
            label = key.replace('_', ' ').title()
            report += f"**{label}**: {val}\n\n"
        report += "---\n\n"

    return report


def analyze_failure(pipeline_run_id=None, batch_id=None):
    context = _gather_failure_context(pipeline_run_id, batch_id)

    if not context:
        return "No failure data found for the given pipeline run or batch."

    if OPENAI_API_KEY and LANGCHAIN_AVAILABLE:
        context_text = _format_context(context)
        return analyze_with_langchain(context_text)
    elif OPENAI_API_KEY and OPENAI_AVAILABLE:
        context_text = _format_context(context)
        return analyze_with_openai(context_text)
    else:
        return analyze_with_rules(context)


def register_genai_routes(app):
    from fastapi import Depends, Query as FQuery
    from src.api.main import verify_api_key

    @app.get("/genai/analyze", tags=["GenAI"], dependencies=[Depends(verify_api_key)])
    async def genai_analyze(
        pipeline_run_id: str = FQuery(None),
        batch_id: str = FQuery(None)
    ):
        if not pipeline_run_id and not batch_id:
            return {"error": "Provide pipeline_run_id or batch_id"}
        result = analyze_failure(pipeline_run_id, batch_id)
        return {"analysis": result, "mode": "llm" if OPENAI_API_KEY else "rule-based"}

    @app.get("/genai/latest-analysis", tags=["GenAI"], dependencies=[Depends(verify_api_key)])
    async def genai_latest():
        with get_cursor() as (cur, conn):
            cur.execute("SELECT pipeline_run_id, batch_id FROM fact_pipeline_runs ORDER BY created_at DESC LIMIT 1")
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
        if not row:
            return {"analysis": "No pipeline runs found."}
        r = dict(zip(cols, row))
        result = analyze_failure(pipeline_run_id=r['pipeline_run_id'], batch_id=r['batch_id'])
        return {"analysis": result, "pipeline_run_id": r['pipeline_run_id'],
                "mode": "llm" if OPENAI_API_KEY else "rule-based"}


if __name__ == "__main__":
    result = analyze_failure()
    print(result)
