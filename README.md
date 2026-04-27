# EnviroQuery 

**A conversational interface for California environmental data — ask questions in plain English, get answers from a real database.**

Built with Python, Streamlit, DuckDB, and the Anthropic Claude API.

---

## What it does

EnviroQuery lets non-technical users query an environmental database without writing a single line of SQL. Type a question like:

> *"Which county had the worst air quality in 2023?"*

And the app:
1. Sends your question to Claude (Anthropic's LLM) along with the database schema
2. Claude translates it into a SQL query
3. DuckDB runs the query against the dataset in-memory
4. Results are displayed as a formatted table with optional visualization

This demonstrates the **text-to-SQL** pattern — one of the core use cases for LLMs in data engineering.

---

## Dataset

Three California environmental tables (2020–2024):

| Table | Rows | Description |
|-------|------|-------------|
| `air_quality` | 2,000 | PM2.5, ozone, AQI by city/county/date |
| `wildfires` | 600 | Acres burned, cause, duration, structures |
| `water_quality` | 800 | pH, turbidity, dissolved oxygen, EPA compliance |

Data is on real California environmental patterns (Fresno/Kern air quality disadvantage, summer fire seasonality, etc).

---

## Example questions to try

- *Which county had the worst average air quality in 2023?*
- *Show the 10 largest wildfires by acres burned*
- *How many wildfires were caused by lightning vs human activity?*
- *What percentage of water samples meet EPA standards by county?*
- *Which months tend to have the highest PM2.5 levels?*
- *Compare average AQI between Los Angeles and San Francisco by year*

---

## Running locally

### 1. Clone the repo

```bash
git clone https://github.com/ianhohsfield/enviro-query.git
cd enviro-query
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate the dataset

```bash
python data/generate_data.py
```

### 4. Run the app

```bash
streamlit run app.py
```

### 5. Add your API key

In the sidebar, paste your [Anthropic API key](https://console.anthropic.com). It's never stored or logged.

---

## Deploying to Streamlit Cloud

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your repo, set `app.py` as the entry point
4. Add `ANTHROPIC_API_KEY` as a secret in the Streamlit Cloud dashboard (optional — users can also enter it in the sidebar)
5. Deploy

---

## Architecture

```
User question
     │
     ▼
Claude API  ◄──── Database schema injected into system prompt
     │
     ▼
Generated SQL
     │
     ▼
DuckDB (in-memory)  ◄──── CSV data loaded at startup
     │
     ▼
Results → Streamlit UI → Table + optional chart + CSV download
```

---

## Stack

- **[Streamlit](https://streamlit.io)** — web UI, no frontend code required
- **[DuckDB](https://duckdb.org)** — fast in-process SQL engine, runs queries on pandas DataFrames
- **[Anthropic Claude API](https://anthropic.com)** — LLM that translates natural language to SQL
- **[pandas](https://pandas.pydata.org)** — data manipulation and CSV loading

---

## Project structure

```
enviro-query/
├── app.py                  # Main Streamlit application
├── requirements.txt
├── .env.example
├── README.md
└── data/
    ├── generate_data.py    # Script to generate synthetic CSV data
    ├── air_quality.csv     # Generated — 2,000 rows
    ├── wildfires.csv       # Generated — 600 rows
    └── water_quality.csv   # Generated — 800 rows
```

---

## Author

Ian Hohsfield


