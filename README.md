# snowflake-cortex-underwriting

AI-powered insurance underwriting portal built natively on Snowflake. Automates end-to-end risk assessment from raw PDF applications to actuarial profitability analysis.
What it does
Underwriters upload PDF applications directly from the UI. The system parses the document, extracts 44+ structured fields across 7 sections (personal data, biometrics, clinical indicators, family history, coverage details), scores the applicant using a trained Snowflake ML regressor, generates a professional underwriter summary, and saves the complete record to Snowflake — all in under 30 seconds.
Architecture

PDF ingestion — Snowflake Internal Stage + CORTEX.AI_PARSE_DOCUMENT with layout-aware OCR
Field extraction — CORTEX.COMPLETE (Mistral Large 2) with structured JSON output across 7 sections
Risk scoring — UNDERWRITING_SCORE_MODEL, a Snowflake ML regressor trained on 16 clinical and financial features, producing a 0–20 risk score mapped to LOW / MEDIUM / HIGH tiers
Underwriter summary — second CORTEX.COMPLETE call generating a 2–3 sentence professional narrative
Actuarial analysis — Python-based mortality model with US life table multipliers, premium pricing, and profitability prediction per policy
Conversational analytics — Cortex Analyst backed by a semantic YAML model for natural language portfolio queries
Frontend — Streamlit in Snowflake with custom IBM Plex typography and Plotly dashboards
