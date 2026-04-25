import streamlit as st
import duckdb
import pandas as pd
import anthropic
import os
import json
import re

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EnviroQuery",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

.stApp {
    background-color: #0f1117;
    color: #e8f0e8;
}

h1, h2, h3 {
    font-family: 'IBM Plex Mono', monospace !important;
    color: #7ecb8f !important;
}

.query-box {
    background: #1a1f2e;
    border: 1px solid #2d4a3e;
    border-left: 3px solid #7ecb8f;
    border-radius: 4px;
    padding: 1rem 1.2rem;
    margin: 0.8rem 0;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 1rem;
    color: #c8e6c9;
}

.sql-block {
    background: #111827;
    border: 1px solid #374151;
    border-radius: 4px;
    padding: 0.8rem 1rem;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.82rem;
    color: #86efac;
    white-space: pre-wrap;
    margin: 0.5rem 0 1rem 0;
    overflow-x: auto;
}

.result-meta {
    font-size: 0.78rem;
    color: #6b7280;
    font-family: 'IBM Plex Mono', monospace;
    margin-bottom: 0.5rem;
}

.example-pill {
    display: inline-block;
    background: #1a2e1f;
    border: 1px solid #2d5a3d;
    border-radius: 20px;
    padding: 0.3rem 0.9rem;
    margin: 0.2rem;
    font-size: 0.82rem;
    color: #86efac;
    cursor: pointer;
}

.schema-table {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    color: #9ca3af;
}

.stButton > button {
    background: #166534 !important;
    color: #dcfce7 !important;
    border: 1px solid #15803d !important;
    border-radius: 4px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.5rem !important;
    transition: all 0.2s !important;
}

.stButton > button:hover {
    background: #15803d !important;
    border-color: #7ecb8f !important;
}

.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background: #1a1f2e !important;
    border: 1px solid #2d4a3e !important;
    color: #e8f0e8 !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    border-radius: 4px !important;
}

.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #7ecb8f !important;
    box-shadow: 0 0 0 1px #7ecb8f22 !important;
}

.stDataFrame {
    border: 1px solid #2d4a3e !important;
}

.stSelectbox > div > div {
    background: #1a1f2e !important;
    border-color: #2d4a3e !important;
    color: #e8f0e8 !important;
}

div[data-testid="stSidebarContent"] {
    background: #0d1117 !important;
    border-right: 1px solid #2d4a3e;
}

.error-box {
    background: #2d1515;
    border: 1px solid #7f1d1d;
    border-radius: 4px;
    padding: 0.8rem 1rem;
    color: #fca5a5;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.82rem;
    margin: 0.5rem 0;
}

.header-bar {
    border-bottom: 1px solid #2d4a3e;
    padding-bottom: 0.5rem;
    margin-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_resource
def load_db():
    con = duckdb.connect(":memory:")
    base = os.path.join(os.path.dirname(__file__), "data")
    con.execute(f"CREATE TABLE air_quality AS SELECT * FROM read_csv_auto('{base}/air_quality.csv')")
    con.execute(f"CREATE TABLE wildfires AS SELECT * FROM read_csv_auto('{base}/wildfires.csv')")
    con.execute(f"CREATE TABLE water_quality AS SELECT * FROM read_csv_auto('{base}/water_quality.csv')")
    return con


# ── Schema definitions (shown to Claude and users) ────────────────────────────
SCHEMA = """
TABLE: air_quality
  date            TEXT       -- ISO date (YYYY-MM-DD)
  county          TEXT       -- California county name
  city            TEXT       -- City within county
  pm25_ugm3       REAL       -- PM2.5 particulate matter (micrograms/m3)
  ozone_ppm       REAL       -- Ozone concentration (parts per million)
  aqi             INTEGER    -- Air Quality Index (0-500)
  aqi_category    TEXT       -- Good / Moderate / Unhealthy for Sensitive Groups / Unhealthy / Very Unhealthy
  year            INTEGER
  month           INTEGER

TABLE: wildfires
  date_started    TEXT       -- ISO date fire began
  county          TEXT       -- California county
  acres_burned    REAL       -- Total acres burned
  duration_days   INTEGER    -- How long the fire burned
  cause           TEXT       -- Lightning / Human / Equipment Use / Debris Burning / Unknown / Arson
  structures_threatened INTEGER
  structures_destroyed  INTEGER
  year            INTEGER
  month           INTEGER

TABLE: water_quality
  date            TEXT       -- ISO date of measurement
  county          TEXT       -- California county
  water_source_type TEXT     -- Reservoir / River / Groundwater / Lake / Stream
  ph              REAL       -- pH level (6-9 range)
  turbidity_ntu   REAL       -- Turbidity in NTU (lower = clearer)
  dissolved_oxygen_mgl REAL  -- Dissolved oxygen mg/L
  nitrate_mgl     REAL       -- Nitrate concentration mg/L
  meets_epa_standard BOOLEAN -- True if all parameters within EPA limits
  year            INTEGER
  month           INTEGER
"""

EXAMPLE_QUESTIONS = [
    "Which county had the worst average air quality in 2023?",
    "Show me the 10 largest wildfires by acres burned",
    "How many wildfires were caused by lightning vs human activity?",
    "What percentage of water samples meet EPA standards by county?",
    "Which months tend to have the highest PM2.5 levels?",
    "Compare average AQI between Los Angeles and San Francisco by year",
    "Which counties have had the most wildfire acres burned since 2022?",
    "Show water quality trends for groundwater sources over time",
]


# ── Claude API call ───────────────────────────────────────────────────────────
def ask_claude(question: str, api_key: str) -> dict:
    client = anthropic.Anthropic(api_key=api_key)

    system_prompt = f"""You are a SQL expert working with a California environmental database.
Convert the user's natural language question into a valid DuckDB SQL query.

DATABASE SCHEMA:
{SCHEMA}

RULES:
- Return ONLY a JSON object with two keys: "sql" and "explanation"
- "sql": a single valid DuckDB SQL query (no markdown, no backticks)
- "explanation": 1-2 sentence plain English description of what the query does
- Use LIMIT 100 unless the user asks for more or a specific number
- For date comparisons use: date >= '2023-01-01'
- Column names are case-sensitive — use exactly as shown in schema
- NEVER include INSERT, UPDATE, DELETE, DROP, or CREATE statements
- If the question cannot be answered with the available data, return:
  {{"sql": "", "explanation": "This question cannot be answered with the available environmental data."}}

Example output format:
{{"sql": "SELECT county, AVG(aqi) as avg_aqi FROM air_quality GROUP BY county ORDER BY avg_aqi DESC LIMIT 10", "explanation": "This finds the 10 counties with the highest average Air Quality Index."}}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=system_prompt,
        messages=[{"role": "user", "content": question}]
    )

    raw = response.content[0].text.strip()
    # Strip markdown fences if present
    raw = re.sub(r"```json\s*|\s*```", "", raw).strip()
    return json.loads(raw)


# ── Main UI ───────────────────────────────────────────────────────────────────
def main():
    con = load_db()

    # Sidebar
    with st.sidebar:
        st.markdown("## 🌿 EnviroQuery")
        st.markdown("*Ask questions about California environmental data in plain English.*")
        st.markdown("---")

        api_key = st.text_input(
            "Anthropic API Key",
            type="password",
            placeholder="sk-ant-...",
            help="Get yours at console.anthropic.com"
        )

        st.markdown("---")
        st.markdown("### 📋 Example Questions")
        for q in EXAMPLE_QUESTIONS:
            st.markdown(f"<div class='example-pill'>→ {q}</div>", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("### 🗂 Database Schema")
        st.markdown(f"<div class='schema-table'><pre>{SCHEMA}</pre></div>", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown(
            "<div style='font-size:0.72rem;color:#4b5563;font-family:IBM Plex Mono,monospace;'>"
            "Built by Ian Hohsfield<br>"
            "Data: Synthetic CA environmental dataset<br>"
            "Stack: Python · Streamlit · DuckDB · Claude API"
            "</div>",
            unsafe_allow_html=True
        )

    # Main area
    st.markdown("<div class='header-bar'>", unsafe_allow_html=True)
    st.markdown("# EnviroQuery")
    st.markdown("**California Environmental Data · Natural Language Interface**")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        "Ask any question about California air quality, wildfires, or water quality in plain English. "
        "The AI translates your question into SQL, runs it against the database, and returns the results."
    )

    # Query input
    question = st.text_area(
        "Your question",
        placeholder="e.g. Which county had the worst air quality in 2023?",
        height=80,
        label_visibility="collapsed"
    )

    col1, col2 = st.columns([1, 5])
    with col1:
        run = st.button("Run Query →")
    with col2:
        show_sql = st.checkbox("Show generated SQL", value=True)

    if run:
        if not api_key:
            st.markdown("<div class='error-box'>⚠ Add your Anthropic API key in the sidebar to run queries.</div>", unsafe_allow_html=True)
            return
        if not question.strip():
            st.markdown("<div class='error-box'>⚠ Please enter a question.</div>", unsafe_allow_html=True)
            return

        with st.spinner("Translating question to SQL..."):
            try:
                result = ask_claude(question, api_key)
            except json.JSONDecodeError as e:
                st.markdown(f"<div class='error-box'>Could not parse Claude's response. Try rephrasing your question.<br>{e}</div>", unsafe_allow_html=True)
                return
            except Exception as e:
                st.markdown(f"<div class='error-box'>API error: {e}</div>", unsafe_allow_html=True)
                return

        sql = result.get("sql", "").strip()
        explanation = result.get("explanation", "")

        if not sql:
            st.info(f"💬 {explanation}")
            return

        # Show the question as a styled box
        st.markdown(f"<div class='query-box'>💬 {question}</div>", unsafe_allow_html=True)

        # Explanation
        st.markdown(f"*{explanation}*")

        # SQL block
        if show_sql:
            st.markdown(f"<div class='sql-block'>{sql}</div>", unsafe_allow_html=True)

        # Run the SQL
        try:
            df = con.execute(sql).df()
            st.markdown(f"<div class='result-meta'>↳ {len(df)} row(s) returned</div>", unsafe_allow_html=True)

            if df.empty:
                st.info("Query returned no results.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Auto-chart if there's a numeric column and a text grouping column
                text_cols = [c for c in df.columns if df[c].dtype == object]
                num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

                if text_cols and num_cols and len(df) <= 30:
                    with st.expander("📊 Visualize results"):
                        chart_col = st.selectbox("Value to chart", num_cols)
                        label_col = st.selectbox("Group by", text_cols)
                        chart_df = df[[label_col, chart_col]].dropna().set_index(label_col)
                        st.bar_chart(chart_df)

                # Download button
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download CSV",
                    csv,
                    "enviroquery_results.csv",
                    "text/csv",
                    use_container_width=False
                )

        except Exception as e:
            st.markdown(f"<div class='error-box'>SQL execution error:<br>{e}</div>", unsafe_allow_html=True)

    # History placeholder encouragement
    if not run:
        st.markdown("---")
        st.markdown("#### Try asking:")
        cols = st.columns(2)
        for i, q in enumerate(EXAMPLE_QUESTIONS[:6]):
            with cols[i % 2]:
                st.markdown(f"<div class='example-pill'>→ {q}</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
