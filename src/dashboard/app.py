"""DataPulse AI - Streamlit Observability Dashboard (SQLite)."""
import sys, os
import streamlit as st
import pandas as pd
import plotly.express as px

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.etl.database import get_cursor

st.set_page_config(page_title="DataPulse AI Dashboard", layout="wide")

st.title("DataPulse AI - Observability Dashboard")
st.markdown("Big Data Quality, Pipeline Monitoring & Root-Cause Analytics")
st.divider()


def load_df(query, params=None):
    with get_cursor() as (cur, conn):
        cur.execute(query, params or ())
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    return pd.DataFrame([dict(zip(cols, r)) for r in rows]) if rows else pd.DataFrame()


# ==================== KPI CARDS ====================
col1, col2, col3, col4 = st.columns(4)

pipeline_df = load_df("SELECT status, COUNT(*) as cnt FROM fact_pipeline_runs GROUP BY status")
total_runs = int(pipeline_df['cnt'].sum()) if not pipeline_df.empty else 0
success_runs = int(pipeline_df[pipeline_df['status'] == 'success']['cnt'].sum()) if not pipeline_df.empty else 0
failed_runs = int(pipeline_df[pipeline_df['status'] == 'failed']['cnt'].sum()) if not pipeline_df.empty else 0
success_rate = round(success_runs / total_runs * 100, 1) if total_runs > 0 else 0

dq_df = load_df("SELECT passed, COUNT(*) as cnt FROM fact_data_quality_results GROUP BY passed")
total_checks = int(dq_df['cnt'].sum()) if not dq_df.empty else 0
passed_checks = int(dq_df[dq_df['passed'] == 1]['cnt'].sum()) if not dq_df.empty else 0
dq_pass_rate = round(passed_checks / total_checks * 100, 1) if total_checks > 0 else 0

quarantine_count = load_df("SELECT COUNT(*) as cnt FROM quarantine_records")
q_cnt = int(quarantine_count['cnt'].iloc[0]) if not quarantine_count.empty else 0

col1.metric("Pipeline Runs", total_runs, f"{success_rate}% success")
col2.metric("Failed Runs", failed_runs, delta_color="inverse")
col3.metric("DQ Pass Rate", f"{dq_pass_rate}%", f"{total_checks} checks")
col4.metric("Quarantined Records", q_cnt, delta_color="inverse")

st.divider()

# ==================== TABS ====================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Pipeline Runs", "Data Quality", "Runtime Trends",
    "Anomalies & SLA", "Orders Summary"
])

with tab1:
    st.subheader("Pipeline Run History")
    runs_df = load_df("""
        SELECT pipeline_run_id, pipeline_name, source_name, start_time, end_time,
               status, input_count, output_count, rejected_count, runtime_seconds, batch_id
        FROM fact_pipeline_runs ORDER BY created_at DESC LIMIT 50
    """)
    if not runs_df.empty:
        status_fig = px.pie(runs_df, names='status', title='Pipeline Status Distribution',
                            color='status', color_discrete_map={'success': '#2ecc71', 'failed': '#e74c3c'})
        st.plotly_chart(status_fig, use_container_width=True)
        st.dataframe(runs_df, use_container_width=True)
    else:
        st.info("No pipeline runs found. Run the ETL pipeline first.")

with tab2:
    st.subheader("Data Quality Results")
    dq_results = load_df("""
        SELECT table_name, rule_name, rule_type, column_name, severity, passed,
               failed_count, total_count, failure_percentage, details, checked_at
        FROM fact_data_quality_results ORDER BY checked_at DESC LIMIT 100
    """)

    if not dq_results.empty:
        c1, c2 = st.columns(2)
        with c1:
            sev = dq_results.groupby(['severity', 'passed']).size().reset_index(name='count')
            fig_sev = px.bar(sev, x='severity', y='count', color='passed',
                             title='DQ Results by Severity', barmode='group')
            st.plotly_chart(fig_sev, use_container_width=True)
        with c2:
            top_failed = dq_results[dq_results['passed'] == 0].groupby('rule_name').size() \
                .reset_index(name='count').sort_values('count', ascending=False).head(10)
            if not top_failed.empty:
                fig_top = px.bar(top_failed, x='count', y='rule_name', orientation='h',
                                 title='Top Failed Rules', color_discrete_sequence=['#e74c3c'])
                st.plotly_chart(fig_top, use_container_width=True)

        failed_only = st.checkbox("Show only failed checks")
        display_df = dq_results[dq_results['passed'] == 0] if failed_only else dq_results
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No DQ results found.")

with tab3:
    st.subheader("Pipeline Runtime Trends")
    runtime_df = load_df("""
        SELECT pipeline_run_id, pipeline_name, start_time, runtime_seconds, status
        FROM fact_pipeline_runs ORDER BY start_time
    """)
    if not runtime_df.empty:
        fig_rt = px.line(runtime_df, x='start_time', y='runtime_seconds', color='status',
                         title='Runtime Over Time (seconds)',
                         color_discrete_map={'success': '#2ecc71', 'failed': '#e74c3c'})
        fig_rt.add_hline(y=300, line_dash="dash", line_color="orange",
                         annotation_text="SLA Threshold (300s)")
        st.plotly_chart(fig_rt, use_container_width=True)
    else:
        st.info("No runtime data.")

with tab4:
    st.subheader("Anomalies & SLA Misses")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Inventory Anomalies**")
        inv_df = load_df("""
            SELECT product_id, warehouse_id, warehouse_region, quantity_on_hand, reorder_level
            FROM raw_inventory
            WHERE quantity_on_hand < 0 OR quantity_on_hand < reorder_level
               OR warehouse_region IS NULL OR warehouse_region = ''
            ORDER BY quantity_on_hand LIMIT 20
        """)
        if not inv_df.empty:
            st.dataframe(inv_df, use_container_width=True)

    with c2:
        st.markdown("**SLA Breaches**")
        sla_df = load_df("""
            SELECT pipeline_run_id, pipeline_name, runtime_seconds, status
            FROM fact_pipeline_runs WHERE runtime_seconds > 300
            ORDER BY runtime_seconds DESC LIMIT 20
        """)
        if not sla_df.empty:
            st.dataframe(sla_df, use_container_width=True)
        else:
            st.success("No SLA breaches!")

    st.markdown("**Quarantined Records by Source**")
    q_df = load_df("""
        SELECT source_table, rule_name, COUNT(*) as count
        FROM quarantine_records GROUP BY source_table, rule_name ORDER BY count DESC
    """)
    if not q_df.empty:
        fig_q = px.bar(q_df, x='source_table', y='count', color='rule_name',
                       title='Quarantined Records by Source & Rule')
        st.plotly_chart(fig_q, use_container_width=True)

with tab5:
    st.subheader("Orders Summary")
    orders_df = load_df("""
        SELECT status, COUNT(*) as order_count, COALESCE(SUM(total_amount),0) as revenue
        FROM fact_orders GROUP BY status
    """)
    if not orders_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            fig_os = px.pie(orders_df, names='status', values='order_count', title='Orders by Status')
            st.plotly_chart(fig_os, use_container_width=True)
        with c2:
            fig_or = px.bar(orders_df, x='status', y='revenue', title='Revenue by Status', color='status')
            st.plotly_chart(fig_or, use_container_width=True)

    channel_df = load_df("""
        SELECT channel, COUNT(*) as orders, COALESCE(SUM(total_amount),0) as revenue
        FROM fact_orders GROUP BY channel ORDER BY revenue DESC
    """)
    if not channel_df.empty:
        fig_ch = px.bar(channel_df, x='channel', y='revenue', color='orders', title='Revenue by Channel')
        st.plotly_chart(fig_ch, use_container_width=True)

st.divider()
st.markdown("**DataPulse AI** v1.0 | Built with Python, FastAPI, SQLite, Streamlit")
