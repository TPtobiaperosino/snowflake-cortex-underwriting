# snowflake-cortex-underwriting

AI-powered insurance underwriting portal built natively on Snowflake. Automates end-to-end risk assessment — from raw PDF applications to actuarial profitability analysis — in under 30 seconds.

---

## What it does

Underwriters upload PDF applications directly from the UI. The system:

1. Parses the document and extracts 44+ structured fields across 7 sections
2. Scores the applicant using a trained Snowflake ML regressor (0–20 risk score → LOW / MEDIUM / HIGH)
3. Generates a professional underwriter narrative summary
4. Runs an actuarial profitability simulation per policy
5. Saves the complete record to Snowflake

---

## Architecture

| Layer | Technology |
|---|---|
| PDF ingestion | Snowflake Internal Stage + `CORTEX.AI_PARSE_DOCUMENT` (layout-aware OCR) |
| Field extraction | `CORTEX.COMPLETE` with Mistral Large 2 — structured JSON across 7 sections |
| Risk scoring | `UNDERWRITING_SCORE_MODEL` — Snowflake ML regressor, 16 clinical & financial features |
| Underwriter summary | Second `CORTEX.COMPLETE` call — 2–3 sentence professional narrative |
| Actuarial analysis | Python mortality model with US life table multipliers, premium pricing, profitability prediction |
| Conversational analytics | Cortex Analyst + semantic YAML model for natural language portfolio queries |
| Frontend | Streamlit in Snowflake with IBM Plex typography and Plotly dashboards |

---

## Tech stack

`Snowflake Cortex` · `Snowflake ML` · `Streamlit in Snowflake` · `Plotly` · `Python` · `Mistral Large 2`

---

## Features

- **Analyse Application** — upload a PDF, get a full risk assessment with triggered clinical factors and a visual score gauge
- **Application History** — portfolio dashboard with BMI vs glucose scatter, risk tier breakdown, and per-applicant detail
- **Profitability** — actuarial simulation per applicant: estimated life expectancy, annual premium, profit margin, and ACCEPT / REFER / DECLINE recommendation
- **Ask Your Data** — natural language queries over the portfolio via Cortex Analyst

---

## Setup

> Requires a Snowflake account with Cortex AI and Snowflake ML enabled.

1. Create the database and schema: `UNDERWRITING_DB.UNDERWRITING_SCHEMA`
2. Create an internal stage: `UNDERWRITING_STAGE`
3. Upload the semantic model YAML to a dedicated stage: `YAML_STAGE/underwriting_model.yaml`
4. Train and register `UNDERWRITING_SCORE_MODEL` using the Snowflake ML Classification/Regression API
5. Deploy `app.py` as a Streamlit in Snowflake app
