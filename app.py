"""
AI Underwriting Assistant — Internal Portal
Powered by Snowflake Cortex AI
"""

import json
import re
import math
import datetime
import _snowflake
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Underwriting Portal · AI Assistant",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Serif:wght@400;600&family=IBM+Plex+Mono:wght@400;500&display=swap');

:root {
    --brand:         #1B3A6B;
    --brand-mid:     #244d8f;
    --brand-light:   #EBF0FA;
    --accent:        #2563EB;
    --bg:            #F1F4F8;
    --surface:       #FFFFFF;
    --surface-2:     #F8FAFC;
    --border:        #D9E2EE;
    --border-strong: #B8C8DE;
    --text-primary:  #0F1C2E;
    --text-body:     #2C3E55;
    --text-muted:    #5B7290;
    --text-faint:    #8FA5BF;
    --danger:        #B91C1C;
    --danger-bg:     #FEF2F2;
    --danger-border: #FECACA;
    --warn:          #92400E;
    --warn-bg:       #FFFBEB;
    --warn-border:   #FDE68A;
    --safe:          #14532D;
    --safe-bg:       #F0FDF4;
    --safe-border:   #BBF7D0;
    --radius:        8px;
    --radius-lg:     12px;
    --shadow-sm:     0 1px 3px rgba(15,28,46,0.08), 0 1px 2px rgba(15,28,46,0.05);
    --shadow:        0 4px 12px rgba(15,28,46,0.10), 0 2px 4px rgba(15,28,46,0.06);
}

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif !important; color: var(--text-body); }
.stApp { background: var(--bg) !important; }
h1, h2, h3, h4, h5, h6 { font-family: 'IBM Plex Serif', serif !important; color: var(--text-primary) !important; }

/* ── Dark-mode safety: force readable text everywhere ── */
.stApp, .stApp p, .stApp span, .stApp div,
.stApp li, .stApp label, .stApp small,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span {
    color: var(--text-primary) !important;
}
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p,
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] li,
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] span,
[data-testid="stChatMessage"] p { color: var(--text-primary) !important; }
[data-testid="stAlert"] p, [data-testid="stAlert"] span { color: inherit !important; }
[data-testid="stExpander"] summary span { color: var(--text-primary) !important; }
[data-baseweb="select"] [data-testid="stMarkdownContainer"] { color: var(--text-primary) !important; }
.stApp .stCaption, [data-testid="stCaptionContainer"] { color: var(--text-muted) !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: var(--brand) !important; border-right: none !important;
    box-shadow: 2px 0 16px rgba(15,28,46,0.18);
}
[data-testid="stSidebar"] * { font-family: 'IBM Plex Sans', sans-serif !important; }
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.12) !important; }
.sb-brand {
    padding: 1.6rem 1.25rem 1.25rem; border-bottom: 1px solid rgba(255,255,255,0.12);
    margin-bottom: 1.25rem; display: flex; align-items: center; gap: 0.75rem;
}
.sb-mark {
    width: 38px; height: 38px; background: rgba(255,255,255,0.15);
    border: 1px solid rgba(255,255,255,0.22); border-radius: 7px;
    display: flex; align-items: center; justify-content: center;
    font-family: 'IBM Plex Serif', serif !important;
    font-size: 0.95rem; font-weight: 600; color: #FFFFFF; flex-shrink: 0;
}
.sb-title { font-size: 0.82rem; font-weight: 600; color: #FFFFFF; line-height: 1.2; }
.sb-subtitle { font-size: 0.67rem; color: rgba(255,255,255,0.5); margin-top: 0.1rem; }
.sb-section {
    font-size: 0.61rem; font-weight: 700; letter-spacing: 0.18em;
    text-transform: uppercase; color: rgba(255,255,255,0.38);
    padding: 0.5rem 1.25rem 0.3rem; margin-top: 0.4rem;
}
.sb-stat {
    margin: 0.3rem 0.75rem; padding: 0.65rem 0.9rem;
    background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.1);
    border-radius: var(--radius);
}
.sb-stat .s-label {
    font-size: 0.65rem; color: rgba(255,255,255,0.48);
    text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 0.18rem;
}
.sb-stat .s-value { font-size: 1.15rem; font-weight: 600; color: #FFFFFF; line-height: 1.1; }
.sb-stat .s-value.danger { color: #FCA5A5; }
.sb-stat .s-value.gold   { color: #FCD34D; }
[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,0.08) !important; color: rgba(255,255,255,0.80) !important;
    border: 1px solid rgba(255,255,255,0.16) !important; font-size: 0.75rem;
    border-radius: var(--radius); width: calc(100% - 1.5rem) !important;
    margin: 0.25rem 0.75rem; transition: background 0.2s; box-shadow: none !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(255,255,255,0.16) !important; transform: none !important;
}

/* ── Page header ── */
.page-header {
    background: var(--surface); border-bottom: 3px solid var(--brand);
    padding: 1rem 1.75rem; margin: -1rem -1rem 1.5rem -1rem;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: var(--shadow-sm);
}
.ph-left { display: flex; align-items: center; gap: 0.85rem; }
.ph-mark {
    width: 40px; height: 40px; background: var(--brand); border-radius: var(--radius);
    display: flex; align-items: center; justify-content: center;
    font-family: 'IBM Plex Serif', serif !important;
    font-size: 1rem; font-weight: 700; color: #FFFFFF !important;
    flex-shrink: 0; box-shadow: 0 0 0 2px rgba(255,255,255,0.25);
}
.ph-title {
    font-family: 'IBM Plex Serif', serif !important; font-size: 1.2rem; font-weight: 600;
    color: var(--text-primary) !important; margin: 0; line-height: 1.15;
}
.ph-sub { font-size: 0.73rem; color: var(--text-muted); margin-top: 0.12rem; }
.ph-badge {
    font-size: 0.67rem; font-weight: 700; letter-spacing: 0.09em; text-transform: uppercase;
    background: var(--brand-light); color: var(--brand); border: 1px solid #C5D5EE;
    border-radius: 999px; padding: 0.28rem 0.85rem;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important; gap: 0.1rem;
    border-bottom: 1px solid var(--border) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important; color: var(--text-muted) !important;
    font-size: 0.82rem !important; font-weight: 500 !important;
    padding: 0.6rem 1.25rem !important; border-radius: 6px 6px 0 0 !important;
    border: 1px solid transparent !important; border-bottom: none !important;
}
.stTabs [aria-selected="true"] {
    background: var(--surface) !important; color: var(--brand) !important;
    font-weight: 600 !important; border-color: var(--border) !important;
    border-bottom-color: var(--surface) !important;
}

/* ── Panel ── */
.panel {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius-lg); box-shadow: var(--shadow-sm);
    padding: 1.25rem 1.5rem; margin: 0.75rem 0;
}
.panel-title {
    font-family: 'IBM Plex Serif', serif !important; font-size: 0.92rem; font-weight: 600;
    color: var(--text-primary); margin-bottom: 0.75rem; padding-bottom: 0.6rem;
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; justify-content: space-between;
}
.panel-body { font-size: 0.875rem; line-height: 1.7; color: var(--text-body) !important; }

/* ── Risk badges ── */
.risk-badge {
    display: inline-flex; align-items: center; gap: 0.4rem; padding: 0.28rem 0.85rem;
    border-radius: 999px; font-size: 0.7rem; font-weight: 700;
    letter-spacing: 0.09em; text-transform: uppercase;
}
.risk-badge.HIGH   { background: var(--danger-bg); color: var(--danger); border: 1px solid var(--danger-border); }
.risk-badge.MEDIUM { background: var(--warn-bg);   color: var(--warn);   border: 1px solid var(--warn-border); }
.risk-badge.LOW    { background: var(--safe-bg);   color: var(--safe);   border: 1px solid var(--safe-border); }
.risk-dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; flex-shrink: 0; }

/* ── Data grid ── */
.data-grid {
    display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.55rem; margin-top: 0.75rem;
}
.df-card {
    background: var(--surface-2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 0.65rem 0.9rem;
}
.df-label {
    font-size: 0.61rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.12em;
    color: var(--text-muted); margin-bottom: 0.22rem;
}
.df-value { font-size: 0.875rem; font-weight: 500; color: var(--text-primary) !important; word-break: break-word; }

/* ── Progress steps ── */
.progress-steps {
    display: flex; margin: 0.75rem 0 1.25rem;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius); overflow: hidden; box-shadow: var(--shadow-sm);
}
.progress-step {
    flex: 1; padding: 0.55rem 0.4rem; text-align: center; font-size: 0.67rem;
    font-weight: 500; color: var(--text-faint); border-right: 1px solid var(--border);
    transition: all 0.25s;
}
.progress-step:last-child { border-right: none; }
.ps-icon { display: block; font-size: 0.9rem; margin-bottom: 0.12rem; }
.progress-step.active { background: var(--brand-light); color: var(--brand); font-weight: 600; }
.progress-step.done   { background: var(--safe-bg);     color: var(--safe); }

/* ── Upload zone ── */
.upload-zone {
    background: var(--surface-2); border: 2px dashed var(--border-strong);
    border-radius: var(--radius-lg); padding: 1.75rem 1.5rem;
    text-align: center; margin-bottom: 1rem;
}
.uz-icon { font-size: 1.6rem; margin-bottom: 0.4rem; display: block; }
.uz-title { font-weight: 600; color: var(--text-primary) !important; font-size: 0.92rem; margin-bottom: 0.25rem; }
.uz-sub { font-size: 0.77rem; color: var(--text-muted) !important; }

/* ── Chat header ── */
.chat-header {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius-lg) var(--radius-lg) 0 0; padding: 0.85rem 1.25rem;
    display: flex; align-items: center; gap: 0.75rem; box-shadow: var(--shadow-sm);
}
.ch-pulse {
    width: 8px; height: 8px; border-radius: 50%; background: #16A34A;
    box-shadow: 0 0 0 0 rgba(22,163,74,0.35); animation: pulse-green 2s infinite;
}
@keyframes pulse-green {
    0%   { box-shadow: 0 0 0 0 rgba(22,163,74,0.35); }
    70%  { box-shadow: 0 0 0 6px rgba(22,163,74,0); }
    100% { box-shadow: 0 0 0 0 rgba(22,163,74,0); }
}
.ch-title {
    font-family: 'IBM Plex Serif', serif !important; font-size: 0.92rem;
    font-weight: 600; color: var(--text-primary);
}
.ch-sub { font-size: 0.7rem; color: var(--text-muted); margin-left: auto; }

/* ── Section heading ── */
.section-heading {
    font-family: 'IBM Plex Serif', serif !important; font-size: 0.92rem; font-weight: 600;
    color: var(--text-primary); margin: 1.25rem 0 0.75rem;
    padding-bottom: 0.45rem; border-bottom: 1px solid var(--border);
}

/* ── Buttons ── */
.stButton > button {
    font-family: 'IBM Plex Sans', sans-serif !important; background: var(--surface) !important;
    color: var(--accent) !important; border: 1px solid #C5D5EE !important;
    border-radius: var(--radius) !important; font-size: 0.78rem !important;
    font-weight: 500 !important; transition: all 0.15s !important;
    box-shadow: var(--shadow-sm) !important;
}
.stButton > button:hover {
    background: var(--brand-light) !important; border-color: var(--accent) !important;
    transform: translateY(-1px) !important; box-shadow: var(--shadow) !important;
}
.primary-btn .stButton > button {
    background: var(--brand) !important; color: #FFFFFF !important;
    border-color: var(--brand) !important; font-weight: 600 !important;
    font-size: 0.82rem !important; padding: 0.55rem 1.4rem !important;
}
.primary-btn .stButton > button:hover {
    background: var(--brand-mid) !important; border-color: var(--brand-mid) !important;
}

/* ── File uploader — force light surface ── */
[data-testid="stFileUploader"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
}
[data-testid="stFileUploader"] * { color: var(--text-body) !important; }
[data-testid="stFileUploaderDropzone"] { background: var(--surface-2) !important; }
[data-testid="stFileUploaderDropzoneInstructions"] span,
[data-testid="stFileUploaderDropzoneInstructions"] p,
[data-testid="stFileUploaderDropzoneInstructions"] small { color: var(--text-muted) !important; }
[data-testid="stFileUploader"] button,
[data-testid="stFileUploaderDropzone"] button {
    background: var(--surface) !important; color: var(--brand) !important;
    border: 1.5px solid var(--brand) !important; border-radius: var(--radius) !important;
    font-weight: 600 !important;
}
[data-testid="stFileUploader"] button:hover,
[data-testid="stFileUploaderDropzone"] button:hover { background: var(--brand-light) !important; }

/* ── Sidebar — all text explicitly white ── */
[data-testid="stSidebar"] .sb-stat .s-label,
[data-testid="stSidebar"] .sb-section,
[data-testid="stSidebar"] .sb-subtitle,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span:not(.s-value),
[data-testid="stSidebar"] small { color: rgba(255,255,255,0.55) !important; }
[data-testid="stSidebar"] .s-value { color: #FFFFFF !important; }
[data-testid="stSidebar"] .s-value.danger { color: #FCA5A5 !important; }
[data-testid="stSidebar"] .s-value.gold   { color: #FCD34D !important; }
[data-testid="stSidebar"] .sb-title       { color: #FFFFFF !important; }

/* ── Streamlit overrides ── */
.stDataFrame {
    border: 1px solid var(--border) !important; border-radius: var(--radius) !important;
    overflow: hidden; box-shadow: var(--shadow-sm) !important;
}
[data-testid="stDataFrame"] th {
    background: var(--surface-2) !important; font-size: 0.67rem !important;
    letter-spacing: 0.08em !important; text-transform: uppercase !important;
    color: var(--text-muted) !important; border-bottom: 1px solid var(--border) !important;
    font-weight: 700 !important;
}
.stAlert { border-radius: var(--radius) !important; font-size: 0.84rem !important; }
hr { border-color: var(--border) !important; }
[data-testid="stSelectbox"] > div > div {
    background: var(--surface) !important; border-color: var(--border) !important;
    border-radius: var(--radius) !important; color: var(--text-body) !important;
    font-size: 0.82rem !important;
}
code, pre { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.8rem !important; }
.stCode { border-radius: var(--radius) !important; }

/* ── Danger button ── */
.danger-btn .stButton > button {
    background: #FEF2F2 !important; color: var(--danger) !important;
    border: 1px solid var(--danger-border) !important;
    font-weight: 600 !important; font-size: 0.75rem !important;
}
.danger-btn .stButton > button:hover {
    background: var(--danger-bg) !important;
    border-color: var(--danger) !important; transform: none !important;
}
.danger-confirm .stButton > button {
    background: var(--danger) !important; color: #FFFFFF !important;
    border-color: var(--danger) !important; font-weight: 700 !important;
    font-size: 0.75rem !important;
}
.danger-confirm .stButton > button:hover {
    background: #991B1B !important; border-color: #991B1B !important;
    transform: none !important;
}
.danger-confirm .stButton > button,
.danger-cancel .stButton > button {
    white-space: nowrap !important; height: 2.2rem !important;
    min-height: 2.2rem !important; padding-top: 0 !important; padding-bottom: 0 !important;
    display: flex !important; align-items: center !important; justify-content: center !important;
}
.danger-cancel .stButton > button {
    background: var(--surface) !important; color: var(--text-muted) !important;
    border: 1px solid var(--border-strong) !important;
    font-weight: 500 !important; font-size: 0.75rem !important;
}

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 99px; }
</style>
""", unsafe_allow_html=True)


# ── Snowpark session ──────────────────────────────────────────────────────────
session = get_active_session()

# ── Constants ─────────────────────────────────────────────────────────────────

RISK_LABEL  = {"LOW": "Low Risk", "MEDIUM": "Medium Risk", "HIGH": "High Risk"}
VALID_TIERS = {"HIGH", "MEDIUM", "LOW"}

STEPS = [
    ("📄", "Parse PDF"),
    ("🔍", "Extract Fields"),
    ("⚖️", "ML Score"),
    ("✍️", "Summarise"),
    ("💾", "Save Record"),
]

SEMANTIC_MODEL = "@UNDERWRITING_DB.UNDERWRITING_SCHEMA.YAML_STAGE/underwriting_model.yaml"
TABLE_FQN      = "UNDERWRITING_DB.UNDERWRITING_SCHEMA.APPLICANTS_SCORED"
STAGE_FQN      = "@UNDERWRITING_DB.UNDERWRITING_SCHEMA.UNDERWRITING_STAGE"


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", "").replace("$", "").strip() or default)
    except (ValueError, TypeError):
        return default


def esc(text) -> str:
    return str(text if text is not None else "").replace("$$", "$ $")


def sanitise_filename(name: str) -> str:
    # Keep only alphanumerics, dots, hyphens, underscores — prevents SQL injection via filename
    safe = re.sub(r'[^\w.\-]', '_', name)
    return re.sub(r'_+', '_', safe)


def parse_json_from_llm(raw: str) -> dict:
    if not raw:
        return {}
    raw = re.sub(r"```(?:json)?", "", raw).strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {}


def normalise_tier(raw: str) -> str:
    upper = raw.strip().upper()
    for tier in ("HIGH", "MEDIUM", "LOW"):
        if tier in upper:
            return tier
    return "UNKNOWN"


def predict_risk_ml(extracted: dict) -> tuple[str, list[str], int]:
    """
    Call Snowflake ML regressor UNDERWRITING_SCORE_MODEL to predict RISK_SCORE (0–20).
    Derives RISK_TIER from the predicted score: LOW 0–6, MEDIUM 7–13, HIGH 14–20.
    11 features — same encoding used in training CSV.
    Falls back to MEDIUM / score 10 if the model is unavailable.
    """
    age      = safe_float(extracted.get("age"))
    bmi      = safe_float(extracted.get("bmi_numeric"))
    sbp      = safe_float(extracted.get("systolic_bp"))
    dbp      = safe_float(extracted.get("diastolic_bp"))
    chol     = safe_float(extracted.get("cholesterol_numeric"))
    glucose  = safe_float(extracted.get("glucose_numeric"))
    income   = safe_float(extracted.get("annual_income")) or safe_float(
        re.sub(r"[^\d.]", "", str(extracted.get("annual_gross_income") or "").split()[0])
        if extracted.get("annual_gross_income") else "0"
    )
    coverage = safe_float(extracted.get("coverage_amount_numeric"))

    # Encode categoricals — same encoding used in training CSV
    smoking = str(extracted.get("smoking_status") or "").lower()
    if any(w in smoking for w in ["current smoker", "currently smokes", "active smoker", "yes"]):
        smoking_enc = 2
    elif any(w in smoking for w in ["former", "ex-smoker", "quit", "stopped"]):
        smoking_enc = 1
    else:
        smoking_enc = 0

    declines = str(extracted.get("prior_application_declines") or "").lower()
    decline_enc = 1 if any(w in declines for w in ["yes", "declined", "rejected", "refused"]) else 0

    family = " ".join([
        str(extracted.get("family_history_father") or ""),
        str(extracted.get("family_history_mother") or ""),
        str(extracted.get("family_history_siblings") or ""),
        str(extracted.get("hereditary_flags") or ""),
    ]).lower()
    family_cvd_enc = 1 if any(kw in family for kw in [
        "heart attack", "stroke", "cardiovascular", "cardiac", "coronary"
    ]) else 0

    # ── New enriched features ─────────────────────────────────────────────────
    conditions_text = str(extracted.get("pre_existing_conditions") or "").lower()
    surgical_text   = str(extracted.get("surgical_and_mental_health_history") or "").lower()
    all_medical     = conditions_text + " " + surgical_text

    # HbA1c numeric
    hba1c_match = re.search(r"hba1c[:\s]*(\d+\.?\d*)\s*%", all_medical)
    hba1c_val   = float(hba1c_match.group(1)) if hba1c_match else 0.0

    # Conditions count — split on common separators
    if conditions_text and conditions_text.strip() not in ("none", "null", "—", "", "n/a"):
        parts = re.split(r"[;,\(\)\n]", conditions_text)
        conditions_count = len([p for p in parts if len(p.strip()) > 5])
    else:
        conditions_count = 0

    # Pending investigations flag
    pending_text = str(extracted.get("pending_investigations") or "").lower().strip().rstrip(".")
    pending_flag = 0 if pending_text in ("none", "null", "—", "", "n/a", "no") or pending_text.startswith("none") else 1

    # Years since most recent diagnosis (best-effort regex)
    years_since = 0.0
    diag_match = re.search(r"(?:dx|diagnosed|diagnosis)[^\d]*(\d{4})", all_medical)
    if diag_match:
        diag_year   = int(diag_match.group(1))
        years_since = round(datetime.datetime.now().year - diag_year, 1)
        years_since = max(0.0, min(years_since, 50.0))

    # On medications flag
    meds_text  = str(extracted.get("current_medications") or "").lower().strip().rstrip(".")
    on_meds    = 0 if meds_text in ("none", "null", "—", "", "n/a", "no") or meds_text.startswith("none") else 1

    try:
        rows = session.sql(f"""
            WITH input_data AS (
                SELECT
                    {age}::FLOAT      AS AGE,
                    {bmi}::FLOAT      AS BMI_NUMERIC,
                    {sbp}::FLOAT      AS SYSTOLIC_BP,
                    {dbp}::FLOAT      AS DIASTOLIC_BP,
                    {chol}::FLOAT     AS CHOLESTEROL_NUMERIC,
                    {glucose}::FLOAT  AS GLUCOSE_NUMERIC,
                    {income}::FLOAT   AS ANNUAL_INCOME,
                    {coverage}::FLOAT AS COVERAGE_AMOUNT_NUMERIC,
                    {smoking_enc}::FLOAT    AS SMOKING_STATUS,
                    {decline_enc}::FLOAT    AS PRIOR_APPLICATION_DECLINES,
                    {family_cvd_enc}::FLOAT AS FAMILY_HISTORY_CVD,
                    {hba1c_val}::FLOAT      AS HBA1C_NUMERIC,
                    {conditions_count}::FLOAT AS CONDITIONS_COUNT,
                    {pending_flag}::FLOAT   AS PENDING_INVESTIGATIONS,
                    {years_since}::FLOAT    AS YEARS_SINCE_DIAGNOSIS,
                    {on_meds}::FLOAT        AS ON_MEDICATIONS
            )
            SELECT UNDERWRITING_DB.UNDERWRITING_SCHEMA.UNDERWRITING_SCORE_MODEL!PREDICT(*) AS pred
            FROM input_data
        """).collect()

        if rows:
            pred = rows[0]["PRED"]
            if isinstance(pred, str):
                pred = json.loads(pred)
            # Regressor output — Snowflake names it predicted_<TARGET_COLUMN>
            raw_score = float(
                pred.get("predicted_RISK_SCORE")
                or pred.get("output_feature_0")
                or 10
            )
            score = max(0, min(round(raw_score), 20))
            if score >= 14:
                tier = "HIGH"
            elif score >= 7:
                tier = "MEDIUM"
            else:
                tier = "LOW"
            return tier, [f"ML regressor prediction — UNDERWRITING_SCORE_MODEL (raw: {raw_score:.1f})"], score

    except Exception as e:
        st.warning(f"ML model prediction failed: {e}")

    return "MEDIUM", ["ML prediction unavailable — defaulted to MEDIUM"], 10


def render_progress(current_step: int) -> str:
    html = '<div class="progress-steps">'
    for i, (icon, label) in enumerate(STEPS):
        cls  = "done" if i < current_step else ("active" if i == current_step else "")
        tick = "✓ " if i < current_step else ""
        html += f'<div class="progress-step {cls}"><span class="ps-icon">{icon}</span>{tick}{label}</div>'
    return html + "</div>"


def render_risk_badge(risk_tier: str) -> str:
    label = RISK_LABEL.get(risk_tier, risk_tier)
    css   = risk_tier if risk_tier in VALID_TIERS else "MEDIUM"
    return f'<span class="risk-badge {css}"><span class="risk-dot"></span>{label}</span>'


def render_data_grid(fields: dict) -> None:
    html = '<div class="data-grid">'
    for label, value in fields.items():
        v = str(value) if value not in (None, "", "N/A", "null") else "—"
        html += (
            f'<div class="df-card">'
            f'<div class="df-label">{label}</div>'
            f'<div class="df-value">{v}</div>'
            f'</div>'
        )
    st.markdown(html + "</div>", unsafe_allow_html=True)


def trunc(value, n: int) -> str:
    """Truncate a string to n chars, appending … if cut."""
    s = str(value) if value not in (None, "", "null") else ""
    return (s[:n] + "…") if len(s) > n else s


# ── Metrics ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def load_summary_metrics() -> dict:
    try:
        rows = session.sql(f"""
            SELECT
                COUNT(*)                                                               AS total,
                COUNT_IF(RISK_TIER = 'HIGH')                                          AS high_count,
                COUNT_IF(RISK_TIER = 'LOW')                                           AS low_count,
                ROUND(COUNT_IF(RISK_TIER='HIGH') / NULLIF(COUNT(*),0) * 100, 1)      AS pct_high,
                ROUND(AVG(COVERAGE_AMOUNT_NUMERIC) / 1000, 0)                         AS avg_coverage_k,
                ROUND(AVG(BMI_NUMERIC), 1)                                            AS avg_bmi,
                COUNT_IF(SMOKING_STATUS ILIKE '%current smoker%')                     AS current_smoker_count,
                COUNT_IF(PRIOR_APPLICATION_DECLINES ILIKE '%yes%')                    AS prior_decline_count
            FROM {TABLE_FQN}
        """).collect()
        m = rows[0] if rows else None
        return {
            "total":                int(m["TOTAL"])                if m else 0,
            "high_count":           int(m["HIGH_COUNT"])           if m else 0,
            "low_count":            int(m["LOW_COUNT"])            if m else 0,
            "pct_high":             float(m["PCT_HIGH"])           if m and m["PCT_HIGH"] else 0.0,
            "avg_coverage":         float(m["AVG_COVERAGE_K"])     if m and m["AVG_COVERAGE_K"] else 0.0,
            "avg_bmi":              float(m["AVG_BMI"])            if m and m["AVG_BMI"] else 0.0,
            "current_smoker_count": int(m["CURRENT_SMOKER_COUNT"]) if m else 0,
            "prior_decline_count":  int(m["PRIOR_DECLINE_COUNT"])  if m else 0,
        }
    except Exception:
        return {
            "total": 0, "high_count": 0, "low_count": 0, "pct_high": 0.0,
            "avg_coverage": 0.0, "avg_bmi": 0.0,
            "current_smoker_count": 0, "prior_decline_count": 0,
        }


def fmt_coverage(value_k: float) -> str:
    sign = "-" if value_k < 0 else ""
    abs_k = abs(value_k)
    if abs_k >= 999.5:
        m = abs_k / 1000
        return f"{sign}${m:.1f}M" if round(m, 1) != round(m) else f"{sign}${m:.0f}M"
    return f"{sign}${abs_k:,.0f}K"


# ── Table + Delete ────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def ensure_table() -> None:
    """Create the APPLICANTS_SCORED table with the full schema if it does not exist."""
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_FQN} (
            FILE_NAME                        VARCHAR,
            UPLOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FULL_LEGAL_NAME                  VARCHAR,
            DATE_OF_BIRTH                    VARCHAR,
            AGE                              FLOAT,
            GENDER                           VARCHAR,
            NATIONALITY                      VARCHAR,
            MARITAL_STATUS                   VARCHAR,
            RESIDENTIAL_ADDRESS              VARCHAR,
            PHONE                            VARCHAR,
            JOB_TITLE                        VARCHAR,
            EMPLOYER                         VARCHAR,
            INDUSTRY_SECTOR                  VARCHAR,
            EMPLOYMENT_TYPE                  VARCHAR,
            ANNUAL_GROSS_INCOME              VARCHAR,
            ANNUAL_INCOME                    FLOAT,
            YEARS_IN_CURRENT_ROLE            VARCHAR,
            OCCUPATIONAL_HAZARDS             VARCHAR,
            HEIGHT                           VARCHAR,
            WEIGHT                           VARCHAR,
            BMI                              VARCHAR,
            BMI_NUMERIC                      FLOAT,
            DRIVING_RECORD                   VARCHAR,
            ALCOHOL_CONSUMPTION              VARCHAR,
            EXERCISE_FREQUENCY               VARCHAR,
            DIETARY_HABITS                   VARCHAR,
            HAZARDOUS_HOBBIES                VARCHAR,
            SMOKING_STATUS                   VARCHAR,
            BLOOD_PRESSURE                   VARCHAR,
            SYSTOLIC_BP                      FLOAT,
            DIASTOLIC_BP                     FLOAT,
            TOTAL_CHOLESTEROL                VARCHAR,
            CHOLESTEROL_NUMERIC              FLOAT,
            FASTING_GLUCOSE                  VARCHAR,
            GLUCOSE_NUMERIC                  FLOAT,
            LAST_MEDICAL_EXAMINATION         VARCHAR,
            CURRENT_MEDICATIONS              VARCHAR,
            KNOWN_ALLERGIES                  VARCHAR,
            PRE_EXISTING_CONDITIONS          VARCHAR,
            HOSPITALISATION_HISTORY          VARCHAR,
            SURGICAL_AND_MENTAL_HEALTH_HISTORY VARCHAR,
            PENDING_INVESTIGATIONS           VARCHAR,
            FAMILY_HISTORY_FATHER            VARCHAR,
            FAMILY_HISTORY_MOTHER            VARCHAR,
            FAMILY_HISTORY_SIBLINGS          VARCHAR,
            FAMILY_HISTORY_PATERNAL_RELATIVES VARCHAR,
            FAMILY_HISTORY_MATERNAL_RELATIVES VARCHAR,
            HEREDITARY_FLAGS                 VARCHAR,
            COVERAGE_REQUESTED               VARCHAR,
            COVERAGE_AMOUNT_NUMERIC          FLOAT,
            POLICY_TYPE                      VARCHAR,
            BENEFICIARY                      VARCHAR,
            EXISTING_LIFE_COVER              VARCHAR,
            REASON_FOR_COVERAGE              VARCHAR,
            PRIOR_APPLICATION_DECLINES       VARCHAR,
            PRIOR_CLAIMS                     VARCHAR,
            MEDICAL_ASSESSMENT_NARRATIVE     VARCHAR,
            FINANCIAL_RISK_NARRATIVE         VARCHAR,
            RISK_TIER                        VARCHAR,
            RISK_SCORE                       FLOAT,
            UNDERWRITER_SUMMARY              VARCHAR,
            UPLOADED_BY                      VARCHAR
        )
    """).collect()


def delete_all_data() -> tuple[int, int, list[str]]:
    errors: list[str] = []
    rows_deleted  = 0
    files_deleted = 0
    try:
        count_rows   = session.sql(f"SELECT COUNT(*) AS n FROM {TABLE_FQN}").collect()
        rows_deleted = int(count_rows[0]["N"]) if count_rows else 0
        session.sql(f"DELETE FROM {TABLE_FQN}").collect()
    except Exception as e:
        errors.append(f"Table delete failed: {e}")
    try:
        stage_bare = STAGE_FQN.lstrip("@")
        file_rows  = session.sql(f"LIST @{stage_bare}").collect()
        files_deleted = len(file_rows)
        if files_deleted > 0:
            session.sql(f"REMOVE @{stage_bare} PATTERN='.*'").collect()
            remaining = session.sql(f"LIST @{stage_bare}").collect()
            if remaining:
                errors.append(f"Stage removal incomplete: {len(remaining)} file(s) still present.")
                files_deleted = files_deleted - len(remaining)
    except Exception as e:
        errors.append(f"Stage removal failed: {e}")
    return rows_deleted, files_deleted, errors


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def analyse_pdf(uploaded_file) -> dict | None:
    step_ph = st.empty()

    safe_name = sanitise_filename(uploaded_file.name)

    # ── Deduplication check ───────────────────────────────────────────────────
    try:
        dup_rows = session.sql(
            f"SELECT UPLOADED_AT, FULL_LEGAL_NAME, RISK_TIER "
            f"FROM {TABLE_FQN} "
            f"WHERE FILE_NAME = $${esc(safe_name)}$$ "
            f"ORDER BY UPLOADED_AT DESC LIMIT 1"
        ).collect()
        if dup_rows:
            prev = dup_rows[0]
            st.warning(
                f"⚠️ **{safe_name}** was already processed on "
                f"**{prev['UPLOADED_AT']}** — "
                f"applicant **{prev['FULL_LEGAL_NAME']}**, "
                f"risk tier **{prev['RISK_TIER']}**. "
                f"Re-running will insert a duplicate record."
            )
    except Exception:
        pass

    # ── Step 0 · Upload ───────────────────────────────────────────────────────
    step_ph.markdown(render_progress(0) + "_Uploading document to Snowflake stage…_", unsafe_allow_html=True)
    try:
        session.file.put_stream(
            uploaded_file, f"{STAGE_FQN}/{safe_name}",
            auto_compress=False, overwrite=True,
        )
        session.sql(f"ALTER STAGE {STAGE_FQN.lstrip('@')} REFRESH").collect()
    except Exception as e:
        st.error(f"**Upload failed.** {e}")
        return None

    # ── Step 1 · Parse PDF ────────────────────────────────────────────────────
    step_ph.markdown(render_progress(1) + "_Extracting document text with AI_PARSE_DOCUMENT…_", unsafe_allow_html=True)
    try:
        rows     = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                TO_FILE('{STAGE_FQN}', '{safe_name}'),
                {{'mode': 'LAYOUT'}}
            ):content::VARCHAR AS raw_text
        """).collect()
        raw_text = (rows[0]["RAW_TEXT"] or "").strip() if rows else ""
    except Exception as e:
        st.error(f"**Document parsing failed.** {e}")
        return None

    if not raw_text:
        st.warning("No text could be extracted from this document. Please check the file.")
        return None

    # ── Step 2 · Extract structured fields ───────────────────────────────────
    step_ph.markdown(render_progress(2) + "_Extracting structured fields…_", unsafe_allow_html=True)

    extract_prompt = f"""You are an insurance data extraction assistant.
Extract the following fields from the 7-section insurance application document below.
Return ONLY a valid JSON object with exactly these keys — no extra text, no markdown fences.

Keys to extract (grouped by section for reference only — return flat JSON):

SECTION 1 — PERSONAL INFORMATION:
  full_legal_name, date_of_birth, age, gender, nationality,
  marital_status, residential_address, phone

SECTION 2 — OCCUPATION & FINANCIAL PROFILE:
  job_title, employer, industry_sector, employment_type,
  annual_gross_income, years_in_current_role, occupational_hazards

SECTION 3 — LIFESTYLE & BIOMETRIC ASSESSMENT:
  height, weight, bmi_text, bmi_numeric, driving_record,
  alcohol_consumption, exercise_frequency, dietary_habits,
  hazardous_hobbies, smoking_status

SECTION 4 — MEDICAL HISTORY & CLINICAL INDICATORS:
  blood_pressure, systolic_bp, diastolic_bp, total_cholesterol,
  cholesterol_numeric, fasting_glucose, glucose_numeric,
  last_medical_examination, current_medications, known_allergies,
  pre_existing_conditions, hospitalisation_history,
  surgical_and_mental_health_history, pending_investigations

SECTION 5 — FAMILY MEDICAL HISTORY:
  family_history_father, family_history_mother, family_history_siblings,
  family_history_paternal_relatives, family_history_maternal_relatives,
  hereditary_flags

SECTION 6 — COVERAGE REQUEST & POLICY DETAILS:
  coverage_requested, coverage_amount_numeric, policy_type,
  beneficiary, existing_life_cover, reason_for_coverage,
  prior_application_declines, prior_claims

SECTION 7 — MEDICAL & FINANCIAL SUMMARY NARRATIVE:
  medical_assessment_narrative, financial_risk_narrative

Extraction rules:
- bmi_numeric: numeric value only (e.g. 29.3)
- systolic_bp: first number from blood pressure only (e.g. 124 from "124 / 80 mmHg")
- diastolic_bp: second number from blood pressure only (e.g. 80 from "124 / 80 mmHg")
- cholesterol_numeric: numeric mg/dL value only (e.g. 198)
- glucose_numeric: numeric mg/dL value only (e.g. 95)
- coverage_amount_numeric: numeric USD value only, no $ or commas (e.g. 500000)
- annual_gross_income: keep as full text string including any supplementary income notes
- smoking_status: keep the full narrative text from the "Tobacco / Smoking Status:" block, not just Yes/No
- surgical_and_mental_health_history: extract full combined narrative block as a single string
- If a field is not present in the document, use null

Document text:
{esc(raw_text)}"""

    try:
        rows = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large2',
                $${esc(extract_prompt)}$$
            ) AS result
        """).collect()
        raw_result = rows[0]["RESULT"] if rows else ""
        extracted: dict = parse_json_from_llm(raw_result)
    except Exception as e:
        st.error(f"**Field extraction failed.** {e}")
        return None

    if not extracted:
        st.warning(f"Field extraction returned no data. Raw output: `{raw_result[:300]}`")
        return None

    # ── Step 3 · Score risk tier via ML model ─────────────────────────────────
    step_ph.markdown(render_progress(3) + "_Scoring risk tier via ML model…_", unsafe_allow_html=True)

    risk_tier, risk_reasons, risk_score = predict_risk_ml(extracted)

    def g(key, fallback="Unknown"):
        v = extracted.get(key)
        return str(v) if v not in (None, "null", "") else fallback

    classify_payload = f"""Applicant: {g('full_legal_name')}
Age: {g('age')} | Gender: {g('gender')} | Marital Status: {g('marital_status')}
Job Title: {g('job_title')} | Industry: {g('industry_sector')} | Employment Type: {g('employment_type')}
Annual Gross Income: {g('annual_gross_income')} | Occupational Hazards: {g('occupational_hazards')}
Height: {g('height')} | Weight: {g('weight')} | BMI: {g('bmi_numeric')}
Driving Record: {g('driving_record')} | Alcohol: {g('alcohol_consumption')}
Exercise Frequency: {g('exercise_frequency')} | Dietary Habits: {g('dietary_habits')}
Hazardous Hobbies: {g('hazardous_hobbies')}
Smoking Status: {g('smoking_status')}
Blood Pressure: {g('blood_pressure')} (Systolic: {g('systolic_bp')})
Total Cholesterol: {g('total_cholesterol')} ({g('cholesterol_numeric')} mg/dL)
Fasting Glucose: {g('fasting_glucose')} ({g('glucose_numeric')} mg/dL)
Last Medical Examination: {g('last_medical_examination')}
Current Medications: {g('current_medications')}
Known Allergies: {g('known_allergies')}
Pre-existing Conditions: {g('pre_existing_conditions')}
Hospitalisation History: {g('hospitalisation_history')}
Surgical & Mental Health History: {g('surgical_and_mental_health_history')}
Pending Investigations: {g('pending_investigations')}
Family History — Father: {g('family_history_father')}
Family History — Mother: {g('family_history_mother')}
Family History — Siblings: {g('family_history_siblings')}
Hereditary Flags: {g('hereditary_flags')}
Coverage Requested: {g('coverage_requested')} (Numeric: {g('coverage_amount_numeric')} USD)
Policy Type: {g('policy_type')} | Existing Life Cover: {g('existing_life_cover')}
Reason for Coverage: {g('reason_for_coverage')}
Prior Application Declines: {g('prior_application_declines')}
Medical Assessment Narrative: {g('medical_assessment_narrative')}
Financial Risk Narrative: {g('financial_risk_narrative')}
Risk Score: {risk_score} points | ML prediction: {"; ".join(risk_reasons)}"""

    # ── Step 4 · Underwriter summary ──────────────────────────────────────────
    step_ph.markdown(render_progress(4) + "_Generating underwriter summary…_", unsafe_allow_html=True)

    summary_prompt = f"""You are a senior insurance underwriter.
Write two to three concise sentences summarising the key risk factors for this applicant
and justifying the assigned risk tier. Be specific and professional.
Refer explicitly to the medical and financial data. Do not use bullet points.

Applicant profile:
{classify_payload}

Assigned risk tier: {risk_tier}"""

    try:
        rows    = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large2',
                $${esc(summary_prompt)}$$
            ) AS summary
        """).collect()
        summary = (rows[0]["SUMMARY"] or "Summary unavailable.").strip() if rows else "Summary unavailable."
    except Exception as e:
        st.error(f"**Summary generation failed.** {e}")
        summary = "Summary generation failed."

    # ── Step 5 · Save to Snowflake ────────────────────────────────────────────
    step_ph.markdown(render_progress(5) + "_Saving record to Snowflake…_", unsafe_allow_html=True)

    annual_income_numeric = safe_float(
        re.sub(r"[^\d.]", "", str(extracted.get("annual_gross_income") or "").split()[0])
    )

    try:
        session.sql(f"""
            INSERT INTO {TABLE_FQN} (
                FILE_NAME,
                FULL_LEGAL_NAME, DATE_OF_BIRTH, AGE, GENDER, NATIONALITY,
                MARITAL_STATUS, RESIDENTIAL_ADDRESS, PHONE,
                JOB_TITLE, EMPLOYER, INDUSTRY_SECTOR, EMPLOYMENT_TYPE,
                ANNUAL_GROSS_INCOME, ANNUAL_INCOME, YEARS_IN_CURRENT_ROLE, OCCUPATIONAL_HAZARDS,
                HEIGHT, WEIGHT, BMI, BMI_NUMERIC, DRIVING_RECORD,
                ALCOHOL_CONSUMPTION, EXERCISE_FREQUENCY, DIETARY_HABITS,
                HAZARDOUS_HOBBIES, SMOKING_STATUS,
                BLOOD_PRESSURE, SYSTOLIC_BP, DIASTOLIC_BP,
                TOTAL_CHOLESTEROL, CHOLESTEROL_NUMERIC,
                FASTING_GLUCOSE, GLUCOSE_NUMERIC,
                LAST_MEDICAL_EXAMINATION, CURRENT_MEDICATIONS, KNOWN_ALLERGIES,
                PRE_EXISTING_CONDITIONS, HOSPITALISATION_HISTORY,
                SURGICAL_AND_MENTAL_HEALTH_HISTORY, PENDING_INVESTIGATIONS,
                FAMILY_HISTORY_FATHER, FAMILY_HISTORY_MOTHER,
                FAMILY_HISTORY_SIBLINGS, FAMILY_HISTORY_PATERNAL_RELATIVES,
                FAMILY_HISTORY_MATERNAL_RELATIVES, HEREDITARY_FLAGS,
                COVERAGE_REQUESTED, COVERAGE_AMOUNT_NUMERIC, POLICY_TYPE,
                BENEFICIARY, EXISTING_LIFE_COVER, REASON_FOR_COVERAGE,
                PRIOR_APPLICATION_DECLINES, PRIOR_CLAIMS,
                MEDICAL_ASSESSMENT_NARRATIVE, FINANCIAL_RISK_NARRATIVE,
                RISK_TIER, RISK_SCORE, UNDERWRITER_SUMMARY, UPLOADED_BY
            ) VALUES (
                $${esc(safe_name)}$$,
                $${esc(extracted.get('full_legal_name'))}$$,
                $${esc(extracted.get('date_of_birth'))}$$,
                {safe_float(extracted.get('age'))},
                $${esc(extracted.get('gender'))}$$,
                $${esc(extracted.get('nationality'))}$$,
                $${esc(extracted.get('marital_status'))}$$,
                $${esc(extracted.get('residential_address'))}$$,
                $${esc(extracted.get('phone'))}$$,
                $${esc(extracted.get('job_title'))}$$,
                $${esc(extracted.get('employer'))}$$,
                $${esc(extracted.get('industry_sector'))}$$,
                $${esc(extracted.get('employment_type'))}$$,
                $${esc(extracted.get('annual_gross_income'))}$$,
                {annual_income_numeric},
                $${esc(extracted.get('years_in_current_role'))}$$,
                $${esc(extracted.get('occupational_hazards'))}$$,
                $${esc(extracted.get('height'))}$$,
                $${esc(extracted.get('weight'))}$$,
                $${esc(extracted.get('bmi_text'))}$$,
                {safe_float(extracted.get('bmi_numeric'))},
                $${esc(extracted.get('driving_record'))}$$,
                $${esc(extracted.get('alcohol_consumption'))}$$,
                $${esc(extracted.get('exercise_frequency'))}$$,
                $${esc(extracted.get('dietary_habits'))}$$,
                $${esc(extracted.get('hazardous_hobbies'))}$$,
                $${esc(extracted.get('smoking_status'))}$$,
                $${esc(extracted.get('blood_pressure'))}$$,
                {safe_float(extracted.get('systolic_bp'))},
                {safe_float(extracted.get('diastolic_bp'))},
                $${esc(extracted.get('total_cholesterol'))}$$,
                {safe_float(extracted.get('cholesterol_numeric'))},
                $${esc(extracted.get('fasting_glucose'))}$$,
                {safe_float(extracted.get('glucose_numeric'))},
                $${esc(extracted.get('last_medical_examination'))}$$,
                $${esc(extracted.get('current_medications'))}$$,
                $${esc(extracted.get('known_allergies'))}$$,
                $${esc(extracted.get('pre_existing_conditions'))}$$,
                $${esc(extracted.get('hospitalisation_history'))}$$,
                $${esc(extracted.get('surgical_and_mental_health_history'))}$$,
                $${esc(extracted.get('pending_investigations'))}$$,
                $${esc(extracted.get('family_history_father'))}$$,
                $${esc(extracted.get('family_history_mother'))}$$,
                $${esc(extracted.get('family_history_siblings'))}$$,
                $${esc(extracted.get('family_history_paternal_relatives'))}$$,
                $${esc(extracted.get('family_history_maternal_relatives'))}$$,
                $${esc(extracted.get('hereditary_flags'))}$$,
                $${esc(extracted.get('coverage_requested'))}$$,
                {safe_float(extracted.get('coverage_amount_numeric'))},
                $${esc(extracted.get('policy_type'))}$$,
                $${esc(extracted.get('beneficiary'))}$$,
                $${esc(extracted.get('existing_life_cover'))}$$,
                $${esc(extracted.get('reason_for_coverage'))}$$,
                $${esc(extracted.get('prior_application_declines'))}$$,
                $${esc(extracted.get('prior_claims'))}$$,
                $${esc(extracted.get('medical_assessment_narrative'))}$$,
                $${esc(extracted.get('financial_risk_narrative'))}$$,
                $${risk_tier}$$,
                {float(risk_score)},
                $${esc(summary)}$$,
                $${esc(session.get_current_user() or 'unknown')}$$
            )
        """).collect()
    except Exception as e:
        st.warning(f"Analysis complete but record could not be saved: {e}")

    step_ph.empty()
    return {
        "extracted":    extracted,
        "risk_tier":    risk_tier,
        "risk_score":   risk_score,
        "risk_reasons": risk_reasons,
        "summary":      summary,
    }


# ── Ensure table exists ───────────────────────────────────────────────────────
try:
    ensure_table()
except Exception as _e:
    st.warning(f"Table setup warning: {_e}")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-brand">
        <div class="sb-mark">UW</div>
        <div>
            <div class="sb-title">Underwriting Portal</div>
            <div class="sb-subtitle">AI-assisted risk assessment</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    metrics = load_summary_metrics()

    st.markdown('<div class="sb-section">Portfolio</div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div class="sb-stat">
        <div class="s-label">Total Applications</div>
        <div class="s-value">{metrics["total"]:,}</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">High-Risk Applicants</div>
        <div class="s-value danger">{metrics["high_count"]:,}</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">High-Risk Rate</div>
        <div class="s-value">{metrics["pct_high"]}%</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">Avg. Coverage Requested</div>
        <div class="s-value">{fmt_coverage(metrics["avg_coverage"])}</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">Avg. BMI</div>
        <div class="s-value">{metrics["avg_bmi"]:.1f}</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">Current Smokers</div>
        <div class="s-value">{metrics["current_smoker_count"]:,}</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">Prior Declines</div>
        <div class="s-value danger">{metrics["prior_decline_count"]:,}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sb-section">System</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="sb-stat">
        <div class="s-label">AI Engine</div>
        <div class="s-value gold" style="font-size:0.85rem">Snowflake Cortex</div>
    </div>
    <div class="sb-stat">
        <div class="s-label">Risk Model</div>
        <div class="s-value" style="font-size:0.78rem">UNDERWRITING_SCORE_MODEL</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↺  Refresh Metrics", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown('<div class="sb-section" style="margin-top:1.25rem">Danger Zone</div>', unsafe_allow_html=True)

    if not st.session_state.get("confirm_delete_sidebar"):
        st.markdown('<div class="danger-btn">', unsafe_allow_html=True)
        if st.button("🗑  Delete All Data", use_container_width=True, key="del_trigger_sb"):
            st.session_state["confirm_delete_sidebar"] = True
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.markdown(
            '<div style="font-size:0.72rem;color:rgba(255,200,200,0.9);'
            'padding:0.4rem 0.75rem 0.3rem;line-height:1.4;">'
            "⚠️ Permanently deletes all records and stage files."
            "</div>",
            unsafe_allow_html=True,
        )
        sb_yes, sb_no = st.columns([1, 1])
        with sb_yes:
            st.markdown('<div class="danger-confirm" style="margin:0 0.2rem 0 0.75rem">', unsafe_allow_html=True)
            if st.button("Delete", use_container_width=True, key="del_confirm_sb"):
                with st.spinner("Deleting…"):
                    rows_del, files_del, errs = delete_all_data()
                st.session_state["confirm_delete_sidebar"] = False
                st.cache_data.clear()
                if errs:
                    st.warning("\n".join(errs))
                else:
                    st.success(f"Deleted {rows_del} record(s) and {files_del} file(s).")
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        with sb_no:
            st.markdown('<div class="danger-cancel" style="margin:0 0.75rem 0 0.2rem">', unsafe_allow_html=True)
            if st.button("Cancel", use_container_width=True, key="del_cancel_sb"):
                st.session_state["confirm_delete_sidebar"] = False
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)


# ── Page header ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
    <div class="ph-left">
        <div class="ph-mark">UW</div>
        <div>
            <div class="ph-title">AI Underwriting Assistant</div>
            <div class="ph-sub">Automated risk assessment · PDF ingestion · Snowflake Cortex AI</div>
        </div>
    </div>
    <div class="ph-badge">Internal Use Only</div>
</div>
""", unsafe_allow_html=True)


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_analyze, tab_history, tab_profit, tab_chat = st.tabs([
    "  Analyze Application  ",
    "  Application History  ",
    "  Profitability  ",
    "  Ask Your Data  ",
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Analyze Application
# ═══════════════════════════════════════════════════════════════════════════════
with tab_analyze:
    st.markdown("""
    <div class="upload-zone">
        <span class="uz-icon">📂</span>
        <div class="uz-title">Upload an insurance application PDF</div>
        <div class="uz-sub">
            Cortex AI will parse the document, extract structured fields,
            score the risk tier via ML model, and generate an underwriter summary automatically.
        </div>
    </div>
    """, unsafe_allow_html=True)

    uploaded_file = st.file_uploader("Select PDF", type=["pdf"], label_visibility="collapsed")

    if uploaded_file:
        st.info(f"**{uploaded_file.name}** — {uploaded_file.size / 1024:.1f} KB · Ready to process")

        col_btn, _ = st.columns([1, 5])
        with col_btn:
            st.markdown('<div class="primary-btn">', unsafe_allow_html=True)
            run = st.button("Run Analysis", use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        if run:
            result = analyse_pdf(uploaded_file)

            if result:
                extracted = result["extracted"]
                risk_tier = result["risk_tier"]
                summary   = result["summary"]

                st.success("Analysis complete — record saved to Snowflake.")

                badge_html   = render_risk_badge(risk_tier)
                risk_reasons = result.get("risk_reasons", [])
                risk_score   = result.get("risk_score", 0)

                bar_color      = {"HIGH": "#B91C1C", "MEDIUM": "#92400E", "LOW": "#14532D"}.get(risk_tier, "#5B7290")
                low_end_pct    = 33.3
                medium_end_pct = 66.6
                score_label    = f"{risk_score} / 20 pts"
                if risk_score <= 6:
                    marker_pct = (risk_score / 6) * 33.3
                elif risk_score <= 13:
                    marker_pct = 33.3 + ((risk_score - 7) / 6) * 33.3
                else:
                    marker_pct = 66.6 + ((risk_score - 14) / 6) * 33.4
                marker_pct_clamped = max(1, min(marker_pct, 99))

                reasons_html = "".join(
                    f'<div style="display:flex;align-items:baseline;gap:0.5rem;'
                    f'padding:0.2rem 0;border-bottom:1px solid var(--border);">'
                    f'<span style="font-size:0.7rem;color:{bar_color};font-weight:700;">▸</span>'
                    f'<span style="font-size:0.8rem;color:var(--text-body);">{r}</span></div>'
                    for r in risk_reasons
                ) if risk_reasons else '<span style="font-size:0.8rem;color:var(--text-muted);">No risk flags triggered.</span>'

                st.markdown(f"""
                <div class="panel">
                    <div class="panel-title">
                        <span>Risk Assessment</span>
                        {badge_html}
                    </div>
                    <div style="margin-bottom:0.85rem;">
                        <div style="display:flex;justify-content:space-between;align-items:baseline;
                             font-size:0.7rem;color:var(--text-muted);margin-bottom:0.55rem;">
                            <span style="font-weight:600;color:var(--text-primary);">Risk Score</span>
                            <span style="font-weight:700;font-size:0.95rem;color:{bar_color};">{score_label}</span>
                        </div>
                        <div style="position:relative;height:10px;border-radius:99px;overflow:visible;
                                    display:flex;gap:2px;">
                            <div style="width:{low_end_pct:.1f}%;height:10px;background:#BBF7D0;
                                        border-radius:99px 0 0 99px;flex-shrink:0;"></div>
                            <div style="width:{medium_end_pct - low_end_pct:.1f}%;height:10px;
                                        background:#FDE68A;flex-shrink:0;"></div>
                            <div style="flex:1;height:10px;background:#FECACA;
                                        border-radius:0 99px 99px 0;"></div>
                            <div style="position:absolute;left:{marker_pct_clamped:.1f}%;top:50%;
                                        transform:translate(-50%,-50%);
                                        width:16px;height:16px;border-radius:50%;
                                        background:{bar_color};border:2.5px solid #fff;
                                        box-shadow:0 1px 4px rgba(0,0,0,0.25);
                                        z-index:2;"></div>
                        </div>
                        <div style="display:grid;grid-template-columns:{low_end_pct:.1f}% {medium_end_pct - low_end_pct:.1f}% 1fr;
                                    font-size:0.62rem;color:var(--text-muted);margin-top:0.4rem;gap:2px;">
                            <span>LOW (0–6)</span>
                            <span style="text-align:center;">MEDIUM (7–13)</span>
                            <span style="text-align:right;">HIGH (14–20)</span>
                        </div>
                    </div>
                    <div style="margin-bottom:0.6rem;font-size:0.72rem;font-weight:700;
                         color:var(--text-muted);letter-spacing:0.1em;text-transform:uppercase;">
                        Triggered Factors
                    </div>
                    {reasons_html}
                    <div style="margin-top:0.85rem;padding-top:0.75rem;border-top:1px solid var(--border);">
                        <div style="font-size:0.72rem;font-weight:700;color:var(--text-muted);
                             letter-spacing:0.1em;text-transform:uppercase;margin-bottom:0.4rem;">
                            Underwriter Summary
                        </div>
                        <div class="panel-body">{summary}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown('<div class="section-heading">Personal Information</div>', unsafe_allow_html=True)
                render_data_grid({
                    "Full Legal Name":  extracted.get("full_legal_name"),
                    "Date of Birth":    extracted.get("date_of_birth"),
                    "Age":              extracted.get("age"),
                    "Gender":           extracted.get("gender"),
                    "Nationality":      extracted.get("nationality"),
                    "Marital Status":   extracted.get("marital_status"),
                })

                st.markdown('<div class="section-heading">Occupation & Financials</div>', unsafe_allow_html=True)
                render_data_grid({
                    "Job Title":           extracted.get("job_title"),
                    "Employer":            extracted.get("employer"),
                    "Annual Gross Income": extracted.get("annual_gross_income"),
                    "Employment Type":     extracted.get("employment_type"),
                })

                st.markdown('<div class="section-heading">Biometrics & Clinical Indicators</div>', unsafe_allow_html=True)
                render_data_grid({
                    "Height":            extracted.get("height"),
                    "Weight":            extracted.get("weight"),
                    "BMI":               extracted.get("bmi_text"),
                    "BMI (numeric)":     extracted.get("bmi_numeric"),
                    "Blood Pressure":    extracted.get("blood_pressure"),
                    "Total Cholesterol": extracted.get("total_cholesterol"),
                    "Fasting Glucose":   extracted.get("fasting_glucose"),
                })

                st.markdown('<div class="section-heading">Lifestyle</div>', unsafe_allow_html=True)
                render_data_grid({
                    "Smoking Status": trunc(extracted.get("smoking_status"), 120),
                    "Alcohol":        extracted.get("alcohol_consumption"),
                    "Exercise":       extracted.get("exercise_frequency"),
                    "Driving Record": extracted.get("driving_record"),
                })

                st.markdown('<div class="section-heading">Medical History</div>', unsafe_allow_html=True)
                render_data_grid({
                    "Pre-existing Conditions": trunc(extracted.get("pre_existing_conditions"), 200),
                    "Current Medications":      extracted.get("current_medications"),
                    "Pending Investigations":   extracted.get("pending_investigations"),
                })

                st.markdown('<div class="section-heading">Coverage Details</div>', unsafe_allow_html=True)
                render_data_grid({
                    "Coverage Requested":         extracted.get("coverage_requested"),
                    "Policy Type":                extracted.get("policy_type"),
                    "Prior Application Declines":  extracted.get("prior_application_declines"),
                })

                st.cache_data.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Application History
# ═══════════════════════════════════════════════════════════════════════════════
with tab_history:
    col_title, col_filter, col_refresh = st.columns([3, 2, 1])
    with col_title:
        st.markdown(
            '<div class="section-heading" style="margin-top:0.5rem">Processed Applications</div>',
            unsafe_allow_html=True,
        )
    with col_filter:
        risk_filter = st.selectbox(
            "Filter by tier", ["All tiers", "HIGH", "MEDIUM", "LOW"],
            label_visibility="collapsed",
        )
    with col_refresh:
        if st.button("↺ Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    if not st.session_state.get("confirm_delete_history"):
        st.markdown('<div class="danger-btn">', unsafe_allow_html=True)
        if st.button("🗑  Delete all records & stage files", key="del_trigger_hist"):
            st.session_state["confirm_delete_history"] = True
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.warning(
            "⚠️ **This will permanently delete all rows from the table and all "
            "files from the Snowflake stage.** This cannot be undone."
        )
        col_yes, col_no, _ = st.columns([1, 1, 5])
        with col_yes:
            st.markdown('<div class="danger-confirm">', unsafe_allow_html=True)
            if st.button("Delete", use_container_width=True, key="del_confirm_hist"):
                with st.spinner("Deleting all records and stage files…"):
                    rows_del, files_del, errs = delete_all_data()
                st.session_state["confirm_delete_history"] = False
                st.cache_data.clear()
                if errs:
                    st.warning("Completed with warnings:\n" + "\n".join(errs))
                else:
                    st.success(f"✅ Deleted **{rows_del}** record(s) and **{files_del}** stage file(s).")
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        with col_no:
            st.markdown('<div class="danger-cancel">', unsafe_allow_html=True)
            if st.button("Cancel", use_container_width=True, key="del_cancel_hist"):
                st.session_state["confirm_delete_history"] = False
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    # ── Load full dataset for dashboard (no tier filter yet) ──────────────────
    try:
        df_all = session.sql(f"""
            SELECT
                FILE_NAME                                        AS "File",
                FULL_LEGAL_NAME                                  AS "Applicant",
                AGE                                              AS "Age",
                RISK_TIER                                        AS "Risk Tier",
                RISK_SCORE                                       AS "Risk Score",
                ANNUAL_INCOME                                    AS "Annual Income",
                BMI_NUMERIC                                      AS "BMI",
                SYSTOLIC_BP                                      AS "Systolic BP",
                GLUCOSE_NUMERIC                                  AS "Glucose",
                CHOLESTEROL_NUMERIC                              AS "Cholesterol",
                LEFT(SMOKING_STATUS, 40)                         AS "Smoker",
                LEFT(PRE_EXISTING_CONDITIONS, 60)                AS "Conditions",
                COVERAGE_AMOUNT_NUMERIC                          AS "Coverage Req.",
                POLICY_TYPE                                      AS "Policy",
                PRIOR_APPLICATION_DECLINES                       AS "Prior Decline",
                UNDERWRITER_SUMMARY                              AS "AI Summary",
                UPLOADED_AT                                      AS "Uploaded"
            FROM {TABLE_FQN}
            ORDER BY UPLOADED_AT DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Could not load records: {e}")
        df_all = None

    if df_all is None or df_all.empty:
        st.info("No applications found. Upload a PDF in the **Analyze Application** tab.")
    else:
        TIER_COLORS  = {"HIGH": "#C0392B", "MEDIUM": "#D4820A", "LOW": "#1A6B3C"}
        TIER_FILL    = {"HIGH": "#F5B7B1", "MEDIUM": "#FAD7A0", "LOW": "#A9DFBF"}
        CORP_BLUE    = "#1B3A6B"
        CORP_GRAY    = "#5B7290"
        CHART_FONT   = dict(family="IBM Plex Sans, Arial", color="#0F1C2E")

        # ── Portfolio metrics ─────────────────────────────────────────────────
        total       = len(df_all)
        high_count  = int((df_all["Risk Tier"] == "HIGH").sum())
        med_count   = int((df_all["Risk Tier"] == "MEDIUM").sum())
        low_count   = int((df_all["Risk Tier"] == "LOW").sum())
        high_pct    = high_count / total * 100 if total else 0
        avg_score   = float(df_all["Risk Score"].mean()) if total else 0
        avg_bmi_val = float(df_all["BMI"].mean()) if "BMI" in df_all.columns else 0
        avg_cov_k   = float(df_all["Coverage Req."].mean() / 1000) if "Coverage Req." in df_all.columns else 0
        smoker_count = int(df_all["Smoker"].str.contains("current|active", case=False, na=False).sum())

        # ── KPI strip ─────────────────────────────────────────────────────────
        st.markdown(f"""
        <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:0.6rem;margin-bottom:1.25rem;">
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid {CORP_BLUE};
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{CORP_GRAY};">Applications</div>
            <div style="font-size:1.75rem;font-weight:700;color:{CORP_BLUE};line-height:1.1;margin-top:0.2rem;">{total}</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #C0392B;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{CORP_GRAY};">High Risk</div>
            <div style="font-size:1.75rem;font-weight:700;color:#C0392B;line-height:1.1;margin-top:0.2rem;">{high_count}</div>
            <div style="font-size:0.7rem;color:{CORP_GRAY};">{high_pct:.0f}% of portfolio</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #D4820A;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{CORP_GRAY};">Medium Risk</div>
            <div style="font-size:1.75rem;font-weight:700;color:#D4820A;line-height:1.1;margin-top:0.2rem;">{med_count}</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #1A6B3C;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{CORP_GRAY};">Low Risk</div>
            <div style="font-size:1.75rem;font-weight:700;color:#1A6B3C;line-height:1.1;margin-top:0.2rem;">{low_count}</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid {CORP_BLUE};
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{CORP_GRAY};">Avg ML Score</div>
            <div style="font-size:1.75rem;font-weight:700;color:{CORP_BLUE};line-height:1.1;margin-top:0.2rem;">{avg_score:.1f}<span style="font-size:0.9rem;color:{CORP_GRAY};">/20</span></div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid {CORP_GRAY};
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{CORP_GRAY};">Avg Coverage</div>
            <div style="font-size:1.75rem;font-weight:700;color:{CORP_BLUE};line-height:1.1;margin-top:0.2rem;">{fmt_coverage(avg_cov_k)}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Row 1: full-width bar chart ──────────────────────────────────────
        ch_bar, = st.columns([1])

        with ch_bar:
            # Ranked applicant bar — primary chart, most insightful
            bar_df = df_all.dropna(subset=["Applicant","Risk Score","Risk Tier"]).copy()
            bar_df = bar_df.sort_values("Risk Score", ascending=True)
            bar_df["Label"] = bar_df["Applicant"].apply(
                lambda x: " ".join(str(x).split()[:2]) if isinstance(x, str) else str(x)
            )
            bar_df["Color"] = bar_df["Risk Tier"].map(TIER_FILL).fillna("#D9E2EE")
            bar_df["Border"]= bar_df["Risk Tier"].map(TIER_COLORS).fillna(CORP_GRAY)

            fig_bar = go.Figure()
            # Background zones
            for x0, x1, fc in [(0,7,"rgba(26,107,60,0.06)"),(7,14,"rgba(212,130,10,0.06)"),(14,20,"rgba(192,57,43,0.06)")]:
                fig_bar.add_vrect(x0=x0, x1=x1, fillcolor=fc, line_width=0, layer="below")

            fig_bar.add_trace(go.Bar(
                y=bar_df["Label"],
                x=bar_df["Risk Score"],
                orientation="h",
                marker=dict(
                    color=bar_df["Color"],
                    line=dict(color=bar_df["Border"], width=1.5)
                ),
                text=bar_df["Risk Score"].apply(lambda v: f"{v:.0f}"),
                textposition="inside",
                textfont=dict(size=11, color="#0F1C2E", family="IBM Plex Sans"),
                hovertemplate="<b>%{customdata[0]}</b><br>ML Score: %{x}/20<br>Tier: %{customdata[1]}<extra></extra>",
                customdata=list(zip(bar_df["Applicant"], bar_df["Risk Tier"])),
            ))
            # Threshold lines — no annotation text, just lines
            for x_val, color in [(7,"#D4820A"),(14,"#C0392B")]:
                fig_bar.add_vline(x=x_val, line_dash="dot", line_color=color,
                                  line_width=1.5, opacity=0.6)
            # Axis labels for thresholds instead of floating text
            fig_bar.update_layout(
                title=dict(text="<b>ML Risk Score by Applicant</b>",
                           font=dict(size=13, color="#0F1C2E", family="IBM Plex Sans"), x=0),
                xaxis=dict(title="Risk Score (0–20)", range=[0, 21],
                           showgrid=True, gridcolor="#F1F4F8", gridwidth=1,
                           zeroline=False,
                           tickvals=[0, 7, 14, 20],
                           ticktext=["0", "7<br><span style='color:#D4820A;font-size:9px'>MED</span>",
                                     "14<br><span style='color:#C0392B;font-size:9px'>HIGH</span>", "20"],
                           tickfont=dict(size=10, color=CORP_GRAY)),
                yaxis=dict(showgrid=False, tickfont=dict(size=11, color="#0F1C2E",
                           family="IBM Plex Sans")),
                margin=dict(t=45, b=55, l=20, r=20),
                height=max(280, 65 * len(bar_df) + 100),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                bargap=0.4,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # ── Row 2: scatter + breakdown ──────────────────────────────────────
        ch_mid, ch_right = st.columns([3, 2])

        with ch_mid:
            # Clinical scatter — BMI vs Glucose, names in hover only (no text overlap)
            cmp_df = df_all.dropna(subset=["BMI","Glucose","Risk Tier"]).copy()
            fig_cmp = go.Figure()
            # Quadrant shading
            fig_cmp.add_hrect(y0=126, y1=450, fillcolor="rgba(192,57,43,0.05)", line_width=0)
            fig_cmp.add_vrect(x0=30, x1=55, fillcolor="rgba(192,57,43,0.05)", line_width=0)

            for tier_val, grp in cmp_df.groupby("Risk Tier"):
                fig_cmp.add_trace(go.Scatter(
                    x=grp["BMI"], y=grp["Glucose"],
                    mode="markers",
                    name=tier_val,
                    marker=dict(
                        size=14,
                        color=TIER_COLORS.get(tier_val, CORP_GRAY),
                        opacity=0.9,
                        symbol="circle",
                        line=dict(color="#fff", width=2)
                    ),
                    hovertemplate="<b>%{customdata}</b><br>BMI: %{x:.1f}<br>Glucose: %{y:.0f} mg/dL<extra></extra>",
                    customdata=cmp_df.loc[grp.index, "Applicant"],
                ))
            # Reference lines — annotation on axis, not floating
            fig_cmp.add_hline(y=126, line_dash="dot", line_color="#C0392B", line_width=1.2)
            fig_cmp.add_vline(x=30, line_dash="dot", line_color="#D4820A", line_width=1.2)
            fig_cmp.update_layout(
                title=dict(text="<b>Clinical Profile — BMI vs. Fasting Glucose</b>",
                           font=dict(size=13, color="#0F1C2E", family="IBM Plex Sans"), x=0),
                xaxis=dict(title="BMI",
                           tickvals=[20, 25, 30, 35, 40, 45, 50],
                           ticktext=["20","25","30*","35","40","45","50"],
                           showgrid=True, gridcolor="#F1F4F8",
                           tickfont=dict(size=10, color=CORP_GRAY)),
                yaxis=dict(title="Fasting Glucose (mg/dL)",
                           tickvals=[60, 100, 126, 200, 300, 400],
                           ticktext=["60","100","126*","200","300","400"],
                           showgrid=True, gridcolor="#F1F4F8",
                           tickfont=dict(size=10, color=CORP_GRAY)),
                margin=dict(t=45, b=80, l=70, r=20),
                height=360,
                legend=dict(title="", orientation="h", y=-0.28, x=0.5,
                            xanchor="center", font=dict(size=11, color="#0F1C2E")),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            # Footnote for reference lines
            st.plotly_chart(fig_cmp, use_container_width=True)
            st.caption("* BMI 30 = obese threshold · Glucose 126 = diabetic threshold")

        with ch_right:
            # Tier breakdown — horizontal stacked, more readable
            tier_counts = df_all["Risk Tier"].value_counts().reindex(["HIGH","MEDIUM","LOW"]).fillna(0)
            fig_tier = go.Figure()
            for t, c, label in [("LOW","#1A6B3C","Low"),("MEDIUM","#D4820A","Medium"),("HIGH","#C0392B","High")]:
                val = int(tier_counts.get(t, 0))
                pct = val / total * 100 if total else 0
                fig_tier.add_trace(go.Bar(
                    name=label,
                    y=["Portfolio"],
                    x=[val],
                    orientation="h",
                    marker_color=c,
                    marker_line=dict(color="#fff", width=1),
                    text=f"{val}  ({pct:.0f}%)" if val > 0 else "",
                    textposition="inside",
                    textfont=dict(size=11, color="#fff", family="IBM Plex Sans"),
                    hovertemplate=f"<b>{label} Risk</b>: {val} ({pct:.0f}%)<extra></extra>",
                ))
            fig_tier.update_layout(
                title=dict(text="<b>Risk Tier Breakdown</b>",
                           font=dict(size=13, color="#0F1C2E", family="IBM Plex Sans"), x=0),
                barmode="stack",
                xaxis=dict(title="Applicants", showgrid=True, gridcolor="#F1F4F8",
                           tickfont=dict(size=10, color=CORP_GRAY)),
                yaxis=dict(showticklabels=False, showgrid=False),
                margin=dict(t=45, b=80, l=10, r=10),
                height=360,
                legend=dict(orientation="h", y=-0.28, x=0.5, xanchor="center",
                            font=dict(size=11, color="#0F1C2E"),
                            traceorder="reversed"),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_tier, use_container_width=True)

        st.markdown('<div class="section-heading">Applicant Records</div>', unsafe_allow_html=True)

        # ── Filter ────────────────────────────────────────────────────────────
        where_filter = risk_filter
        df_show = df_all if where_filter == "All tiers" else df_all[df_all["Risk Tier"] == where_filter]

        TIER_PILL = {
            "HIGH":   '<span style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.18rem 0.65rem;border-radius:999px;font-size:0.68rem;font-weight:700;letter-spacing:0.07em;background:#FEF2F2;color:#B91C1C;border:1px solid #FECACA;">● HIGH</span>',
            "MEDIUM": '<span style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.18rem 0.65rem;border-radius:999px;font-size:0.68rem;font-weight:700;letter-spacing:0.07em;background:#FFFBEB;color:#92400E;border:1px solid #FDE68A;">● MEDIUM</span>',
            "LOW":    '<span style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.18rem 0.65rem;border-radius:999px;font-size:0.68rem;font-weight:700;letter-spacing:0.07em;background:#F0FDF4;color:#14532D;border:1px solid #BBF7D0;">● LOW</span>',
        }

        def score_bar_html(score, tier):
            color = TIER_COLORS.get(tier, "#5B7290")
            if score <= 6:
                pct = (score / 6) * 33.3
            elif score <= 13:
                pct = 33.3 + ((score - 7) / 6) * 33.3
            else:
                pct = 66.6 + ((score - 14) / 6) * 33.4
            pct = max(1, min(pct, 99))
            return f'''<div style="display:flex;align-items:center;gap:0.5rem;min-width:120px;">
                <div style="position:relative;flex:1;height:6px;border-radius:99px;
                            background:linear-gradient(to right,#BBF7D0 33%,#FDE68A 33% 66%,#FECACA 66%);">
                    <div style="position:absolute;left:{pct:.0f}%;top:50%;
                                transform:translate(-50%,-50%);width:10px;height:10px;
                                border-radius:50%;background:{color};border:1.5px solid #fff;
                                box-shadow:0 1px 3px rgba(0,0,0,0.2);"></div>
                </div>
                <span style="font-size:0.72rem;font-weight:600;color:{color};white-space:nowrap;">{score:.0f}/20</span>
            </div>'''

        # Build HTML table
        rows_html = ""
        for _, row in df_show.iterrows():
            tier_raw  = str(row.get("Risk Tier") or "LOW").upper()
            score_val = float(row.get("Risk Score") or 0)
            income    = row.get("Annual Income") or 0
            coverage  = row.get("Coverage Req.") or 0
            bmi       = row.get("BMI") or 0
            glucose   = row.get("Glucose") or 0
            uploaded  = row.get("Uploaded")
            uploaded_str = uploaded.strftime("%d %b %Y") if hasattr(uploaded, "strftime") else str(uploaded)[:10]
            prior     = str(row.get("Prior Decline") or "No")
            prior_html = f'<span style="color:#B91C1C;font-weight:600;">Yes</span>' if "yes" in prior.lower() else '<span style="color:#14532D;">No</span>'
            conditions = str(row.get("Conditions") or "—")[:55] + ("…" if len(str(row.get("Conditions") or "")) > 55 else "")

            rows_html += f"""
            <tr style="border-bottom:1px solid #F1F4F8;">
                <td style="padding:0.75rem 0.9rem;white-space:nowrap;">
                    <div style="font-weight:600;color:#0F1C2E;font-size:0.82rem;">{row.get("Applicant","—")}</div>
                    <div style="font-size:0.7rem;color:#5B7290;margin-top:0.1rem;">{row.get("Policy","—")} · Age {int(row.get("Age") or 0)}</div>
                </td>
                <td style="padding:0.75rem 0.9rem;">{TIER_PILL.get(tier_raw, tier_raw)}</td>
                <td style="padding:0.75rem 0.9rem;">{score_bar_html(score_val, tier_raw)}</td>
                <td style="padding:0.75rem 0.9rem;font-size:0.8rem;color:#2C3E55;">
                    <div>${income:,.0f}</div>
                    <div style="font-size:0.68rem;color:#5B7290;">→ ${coverage:,.0f} coverage</div>
                </td>
                <td style="padding:0.75rem 0.9rem;font-size:0.8rem;color:#2C3E55;">
                    <div>BMI {bmi:.1f}</div>
                    <div style="font-size:0.68rem;color:#5B7290;">Gluc {glucose:.0f} mg/dL</div>
                </td>
                <td style="padding:0.75rem 0.9rem;font-size:0.75rem;color:#5B7290;max-width:200px;">{conditions}</td>
                <td style="padding:0.75rem 0.9rem;text-align:center;">{prior_html}</td>
                <td style="padding:0.75rem 0.9rem;font-size:0.72rem;color:#5B7290;white-space:nowrap;">{uploaded_str}</td>
            </tr>"""

        table_html = f"""
        <div style="background:#fff;border:1px solid #D9E2EE;border-radius:10px;
                    overflow:hidden;box-shadow:0 1px 3px rgba(15,28,46,0.08);">
            <table style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr style="background:#F8FAFC;border-bottom:2px solid #D9E2EE;">
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Applicant</th>
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Tier</th>
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">ML Score</th>
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Income / Coverage</th>
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Biometrics</th>
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Conditions</th>
                        <th style="padding:0.65rem 0.9rem;text-align:center;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Prior Decline</th>
                        <th style="padding:0.65rem 0.9rem;text-align:left;font-size:0.65rem;
                                   font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                                   color:#5B7290;">Uploaded</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
        <div style="font-size:0.72rem;color:#5B7290;margin-top:0.5rem;padding-left:0.2rem;">
            {len(df_show):,} record{"s" if len(df_show) != 1 else ""} displayed
        </div>
        """
        st.markdown(table_html, unsafe_allow_html=True)

        # ── Per-applicant risk bar expander ────────────────────────────────────
        st.markdown('<div class="section-heading">Individual Risk Detail</div>', unsafe_allow_html=True)
        for _, row in df_show.iterrows():
            applicant  = row.get("Applicant", "Unknown")
            score      = float(row.get("Risk Score") or 0)
            tier_raw   = str(row.get("Risk Tier") or "LOW").upper()
            bar_color  = TIER_COLORS.get(tier_raw, "#5B7290")
            badge_html = f'<span class="risk-badge {tier_raw}"><span class="risk-dot"></span>{RISK_LABEL.get(tier_raw, tier_raw)}</span>'

            if score <= 6:
                marker_pct = (score / 6) * 33.3
            elif score <= 13:
                marker_pct = 33.3 + ((score - 7) / 6) * 33.3
            else:
                marker_pct = 66.6 + ((score - 14) / 6) * 33.4
            marker_pct = max(1, min(marker_pct, 99))

            with st.expander(f"{applicant}  —  Score {score:.0f}/20  ·  {tier_raw}"):
                st.markdown(f"""
                <div style="margin-bottom:0.75rem;display:flex;align-items:center;
                            justify-content:space-between;">
                    <span style="font-size:0.85rem;font-weight:600;color:#0F1C2E;">{applicant}</span>
                    {badge_html}
                </div>
                <div style="display:flex;justify-content:space-between;align-items:baseline;
                     font-size:0.7rem;color:#5B7290;margin-bottom:0.4rem;">
                    <span style="font-weight:600;color:#0F1C2E;">ML Risk Score</span>
                    <span style="font-weight:700;font-size:0.9rem;color:{bar_color};">{score:.0f} / 20 pts</span>
                </div>
                <div style="position:relative;height:10px;border-radius:99px;
                            display:flex;gap:2px;overflow:visible;">
                    <div style="width:33.3%;height:10px;background:#BBF7D0;border-radius:99px 0 0 99px;"></div>
                    <div style="width:33.3%;height:10px;background:#FDE68A;"></div>
                    <div style="flex:1;height:10px;background:#FECACA;border-radius:0 99px 99px 0;"></div>
                    <div style="position:absolute;left:{marker_pct:.1f}%;top:50%;
                                transform:translate(-50%,-50%);width:14px;height:14px;
                                border-radius:50%;background:{bar_color};border:2px solid #fff;
                                box-shadow:0 1px 4px rgba(0,0,0,0.25);z-index:2;"></div>
                </div>
                <div style="display:flex;justify-content:space-between;font-size:0.62rem;
                     color:#5B7290;margin-top:0.3rem;">
                    <span>LOW (0–6)</span><span>MEDIUM (7–13)</span><span>HIGH (14–20)</span>
                </div>
                <div style="margin-top:0.75rem;font-size:0.8rem;color:#2C3E55;
                            line-height:1.6;padding-top:0.6rem;border-top:1px solid #D9E2EE;">
                    {row.get("AI Summary","—")}
                </div>
                """, unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Age",        f"{row.get('Age','—')}")
                c2.metric("BMI",        f"{row.get('BMI', 0):.1f}")
                c3.metric("Glucose",    f"{row.get('Glucose', 0):.0f} mg/dL")
                c4.metric("Coverage",   fmt_coverage((row.get('Coverage Req.') or 0)/1000))


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Profitability Analysis  (SQL actuarial functions)
# ═══════════════════════════════════════════════════════════════════════════════

def predict_years_to_death(age, bmi, sbp, glucose, smoking_enc, conditions_count,
                            hba1c, decline, family_cvd, on_meds, pending):
    """
    Actuarial function — estimates remaining years of life.
    Based on US life table 2020 base + calibrated clinical mortality multipliers.
    """
    if age < 25:   base = 57.0
    elif age < 30: base = 52.0
    elif age < 35: base = 47.0
    elif age < 40: base = 42.0
    elif age < 45: base = 37.0
    elif age < 50: base = 32.0
    elif age < 55: base = 27.0
    elif age < 60: base = 23.0
    elif age < 65: base = 19.0
    elif age < 70: base = 15.0
    elif age < 75: base = 11.5
    elif age < 80: base = 8.5
    else:          base = 5.5

    m = 1.0
    # BMI (WHO mortality data)
    if bmi >= 40:   m *= 0.78
    elif bmi >= 35: m *= 0.87
    elif bmi >= 30: m *= 0.94
    # Blood pressure
    if sbp >= 180:   m *= 0.70
    elif sbp >= 160: m *= 0.80
    elif sbp >= 140: m *= 0.91
    # Glucose
    if glucose >= 300:   m *= 0.65
    elif glucose >= 200: m *= 0.78
    elif glucose >= 126: m *= 0.88
    # HbA1c — granular, early T2DM less severe
    if hba1c >= 10.0:  m *= 0.75
    elif hba1c >= 9.0: m *= 0.83
    elif hba1c >= 8.0: m *= 0.90
    elif hba1c >= 7.0: m *= 0.95
    # Smoking
    if smoking_enc == 2:   m *= 0.75
    elif smoking_enc == 1: m *= 0.90
    # Prior decline
    if decline == 1:    m *= 0.82
    # Family CVD
    if family_cvd == 1: m *= 0.95
    # Concurrent conditions
    if conditions_count >= 5:   m *= 0.55
    elif conditions_count >= 4: m *= 0.65
    elif conditions_count >= 3: m *= 0.78
    elif conditions_count >= 2: m *= 0.88
    elif conditions_count == 1: m *= 0.95
    if on_meds == 1: m *= 0.97
    if pending == 1: m *= 0.98

    return round(max(0.5, min(base * m, 105 - age)), 1)


def run_profitability_analysis(extracted: dict, risk_score: int,
                                policy_type: str, coverage: float,
                                income: float) -> dict:
    """
    Actuarial profitability analysis.
    Term Life: profitable if net = premiums_collected - payout > 0
               (payout = 0 if insured survives beyond term)
    Whole Life: payout guaranteed; profitable if total_premiums > coverage
    """
    age      = safe_float(extracted.get("age"))
    bmi      = safe_float(extracted.get("bmi_numeric"))
    sbp      = safe_float(extracted.get("systolic_bp"))
    glucose  = safe_float(extracted.get("glucose_numeric"))

    smoking = str(extracted.get("smoking_status") or "").lower()
    smoking_enc = 2 if any(w in smoking for w in ["current smoker","active smoker"]) else \
                  1 if any(w in smoking for w in ["former","ex-smoker","quit"]) else 0

    decline_enc = 1 if "yes" in str(extracted.get("prior_application_declines") or "").lower() else 0
    family = " ".join([
        str(extracted.get("family_history_father") or ""),
        str(extracted.get("family_history_mother") or ""),
        str(extracted.get("family_history_siblings") or ""),
    ]).lower()
    family_enc = 1 if any(kw in family for kw in ["heart attack","stroke","cardiovascular","coronary"]) else 0

    cond_text   = str(extracted.get("pre_existing_conditions") or "").lower()
    all_medical = cond_text + " " + str(extracted.get("surgical_and_mental_health_history") or "").lower()
    cond_count  = len([p for p in re.split(r"[;,\(\)\n]", cond_text) if len(p.strip()) > 5]) \
                  if cond_text.strip() not in ("none","null","","n/a") else 0
    hba1c_match = re.search(r"hba1c[:\s]*(\d+\.?\d*)\s*%", all_medical)
    hba1c_val   = float(hba1c_match.group(1)) if hba1c_match else 0.0
    pending_text = str(extracted.get("pending_investigations") or "").lower().strip().rstrip(".")
    pending_flag = 0 if pending_text in ("none","null","—","","n/a","no") or pending_text.startswith("none") else 1
    meds_text   = str(extracted.get("current_medications") or "").lower().strip().rstrip(".")
    on_meds     = 0 if meds_text in ("none","null","—","","n/a","no") or meds_text.startswith("none") else 1

    years_to_death = predict_years_to_death(
        age, bmi, sbp, glucose, smoking_enc, cond_count,
        hba1c_val, decline_enc, family_enc, on_meds, pending_flag
    )

    base_rate = 0.5 * math.exp(0.055 * max(age - 25, 0))
    risk_mult = 1.0 + (risk_score / 20)

    term_map    = {"10": 10, "20": 20, "25": 25, "30": 30}
    policy_term = next((v for k, v in term_map.items() if k in policy_type), 0)

    if policy_term > 0:
        term_adj = 1.0 + (policy_term - 10) * 0.02
        annual_premium = round((coverage / 1000) * base_rate * risk_mult * term_adj, 2)
        years_paying   = min(years_to_death, policy_term)
        total_premiums = round(annual_premium * years_paying, 0)
        payout         = coverage if years_to_death <= policy_term else 0
        net_outcome    = round(total_premiums - payout, 0)
        profitable     = net_outcome > 0
    else:
        annual_premium = round((coverage / 1000) * base_rate * 8 * risk_mult, 2)
        years_paying   = years_to_death
        total_premiums = round(annual_premium * years_paying, 0)
        payout         = coverage
        net_outcome    = round(total_premiums - payout, 0)
        profitable     = net_outcome > 0

    profit_margin = round(net_outcome / max(total_premiums, 1) * 100, 1)

    prem_pct = annual_premium / max(income, 1) * 100

    if profitable and prem_pct < 30:
        recommendation = "ACCEPT"
    elif profitable and prem_pct < 60:
        recommendation = "REFER"
    else:
        recommendation = "DECLINE"

    return {
        "years_to_death":    years_to_death,
        "annual_premium":    annual_premium,
        "profit_margin":     profit_margin,
        "policy_term":       policy_term,
        "years_paying":      round(years_paying, 1),
        "total_premiums":    total_premiums,
        "payout":            payout,
        "net_outcome":       net_outcome,
        "profitable":        profitable,
        "coverage":          coverage,
        "policy_type":       policy_type,
        "prem_income_pct":   round(prem_pct, 1),
        "recommendation":    recommendation,
        "income":            income,
        "_base_rate":        round(base_rate, 3),
        "_risk_mult":        round(risk_mult, 2),
        "_term_adj":         round(term_adj, 2) if policy_term > 0 else None,
        "_wl_mult":          8 if policy_term == 0 else None,
    }


with tab_profit:
    st.markdown('<div class="section-heading" style="margin-top:0.5rem">Actuarial Profitability Analysis</div>',
                unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:0.82rem;color:#5B7290;margin-bottom:1.25rem;line-height:1.6;">
        This tab runs an actuarial simulation per applicant using ML-predicted mortality
        and rate-per-$1,000 pricing. It estimates expected years of life, annual premium,
        profit margin, and whether the policy is projected to be profitable for the insurer.
    </div>
    """, unsafe_allow_html=True)

    try:
        df_profit = session.sql(f"""
            SELECT
                FULL_LEGAL_NAME, AGE, RISK_TIER, RISK_SCORE,
                BMI_NUMERIC, SYSTOLIC_BP, GLUCOSE_NUMERIC, CHOLESTEROL_NUMERIC,
                SMOKING_STATUS, PRE_EXISTING_CONDITIONS, SURGICAL_AND_MENTAL_HEALTH_HISTORY,
                PENDING_INVESTIGATIONS, CURRENT_MEDICATIONS,
                PRIOR_APPLICATION_DECLINES,
                FAMILY_HISTORY_FATHER, FAMILY_HISTORY_MOTHER, FAMILY_HISTORY_SIBLINGS,
                HEREDITARY_FLAGS, COVERAGE_AMOUNT_NUMERIC, ANNUAL_INCOME,
                POLICY_TYPE, UPLOADED_AT
            FROM {TABLE_FQN}
            ORDER BY RISK_SCORE DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Could not load records: {e}")
        df_profit = None

    if df_profit is None or df_profit.empty:
        st.info("No applications found. Upload a PDF in the **Analyze Application** tab.")
    else:
        # Run analysis for each applicant
        results = []
        for _, row in df_profit.iterrows():
            extracted_proxy = {
                "age":                          row.get("AGE"),
                "bmi_numeric":                  row.get("BMI_NUMERIC"),
                "systolic_bp":                  row.get("SYSTOLIC_BP"),
                "glucose_numeric":              row.get("GLUCOSE_NUMERIC"),
                "cholesterol_numeric":          row.get("CHOLESTEROL_NUMERIC"),
                "smoking_status":               row.get("SMOKING_STATUS"),
                "pre_existing_conditions":      row.get("PRE_EXISTING_CONDITIONS"),
                "surgical_and_mental_health_history": row.get("SURGICAL_AND_MENTAL_HEALTH_HISTORY"),
                "pending_investigations":       row.get("PENDING_INVESTIGATIONS"),
                "current_medications":          row.get("CURRENT_MEDICATIONS"),
                "prior_application_declines":   row.get("PRIOR_APPLICATION_DECLINES"),
                "family_history_father":        row.get("FAMILY_HISTORY_FATHER"),
                "family_history_mother":        row.get("FAMILY_HISTORY_MOTHER"),
                "family_history_siblings":      row.get("FAMILY_HISTORY_SIBLINGS"),
                "hereditary_flags":             row.get("HEREDITARY_FLAGS"),
            }
            coverage    = float(row.get("COVERAGE_AMOUNT_NUMERIC") or 0)
            income      = float(row.get("ANNUAL_INCOME") or 0)
            policy_type = str(row.get("POLICY_TYPE") or "Term Life - 20 Year")
            risk_score  = int(row.get("RISK_SCORE") or 0)

            ana = run_profitability_analysis(extracted_proxy, risk_score, policy_type, coverage, income)
            ana["name"]       = row.get("FULL_LEGAL_NAME", "Unknown")
            ana["age"]        = float(row.get("AGE") or 0)
            ana["risk_tier"]  = str(row.get("RISK_TIER") or "LOW")
            ana["risk_score"] = risk_score
            results.append(ana)

        # ── Portfolio KPIs ─────────────────────────────────────────────────────
        n_profit = sum(1 for r in results if r["profitable"])
        n_loss   = len(results) - n_profit
        total_prem = sum(r["total_premiums"] for r in results)
        total_cov  = sum(r["coverage"] for r in results)
        avg_ytd    = sum(r["years_to_death"] for r in results) / len(results)

        st.markdown(f"""
        <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:0.6rem;margin-bottom:1.25rem;">
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #1A6B3C;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#5B7290;">Profitable Policies</div>
            <div style="font-size:1.75rem;font-weight:700;color:#1A6B3C;line-height:1.1;margin-top:0.2rem;">{n_profit}</div>
            <div style="font-size:0.7rem;color:#5B7290;">{n_profit/len(results)*100:.0f}% of portfolio</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #C0392B;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#5B7290;">Expected Losses</div>
            <div style="font-size:1.75rem;font-weight:700;color:#C0392B;line-height:1.1;margin-top:0.2rem;">{n_loss}</div>
            <div style="font-size:0.7rem;color:#5B7290;">{n_loss/len(results)*100:.0f}% of portfolio</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #1B3A6B;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#5B7290;">Total Premiums Est.</div>
            <div style="font-size:1.5rem;font-weight:700;color:#1B3A6B;line-height:1.1;margin-top:0.2rem;">{fmt_coverage(total_prem/1000)}</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #1B3A6B;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#5B7290;">Total Exposure</div>
            <div style="font-size:1.5rem;font-weight:700;color:#1B3A6B;line-height:1.1;margin-top:0.2rem;">{fmt_coverage(total_cov/1000)}</div>
          </div>
          <div style="background:#fff;border:1px solid #D9E2EE;border-top:3px solid #5B7290;
                      border-radius:6px;padding:0.9rem 1rem;">
            <div style="font-size:0.58rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#5B7290;">Avg Life Expectancy</div>
            <div style="font-size:1.75rem;font-weight:700;color:#1B3A6B;line-height:1.1;margin-top:0.2rem;">{avg_ytd:.1f} <span style="font-size:0.9rem;color:#5B7290;">yrs</span></div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Applicant selector ─────────────────────────────────────────────────
        st.markdown('<div class="section-heading">Applicant Profitability Prediction</div>',
                    unsafe_allow_html=True)

        names = [r["name"] for r in results]
        selected_name = st.selectbox(
            "Select applicant", names,
            label_visibility="collapsed",
            placeholder="Choose an applicant…"
        )

        r = next((x for x in results if x["name"] == selected_name), None)

        if r:
            col_btn, _ = st.columns([1, 5])
            with col_btn:
                st.markdown('<div class="primary-btn">', unsafe_allow_html=True)
                run_pred = st.button("Run Profitability Prediction", use_container_width=True, key="run_profit")
                st.markdown("</div>", unsafe_allow_html=True)

            if run_pred:
                profitable  = r["profitable"]
                rec         = r.get("recommendation", "DECLINE")
                prem_pct    = r.get("prem_income_pct", 0)
                net         = r.get("net_outcome", 0)
                payout      = r.get("payout", r["coverage"])

                color  = "#1A6B3C" if profitable else "#C0392B"
                bg     = "#F0FDF4" if profitable else "#FEF2F2"
                border = "#BBF7D0" if profitable else "#FECACA"
                label  = "✓  PROFITABLE" if profitable else "✗  EXPECTED LOSS"
                tier_color = {"HIGH":"#C0392B","MEDIUM":"#D4820A","LOW":"#1A6B3C"}.get(r["risk_tier"],"#5B7290")

                # Affordability context under premium
                if prem_pct > 100:
                    afford_text = f'<div style="font-size:0.67rem;color:#C0392B;margin-top:0.2rem;">{prem_pct:.0f}× income — commercially impossible</div>'
                elif prem_pct > 50:
                    afford_text = f'<div style="font-size:0.67rem;color:#D4820A;margin-top:0.2rem;">{prem_pct:.0f}% of income — unaffordable</div>'
                elif prem_pct > 30:
                    afford_text = f'<div style="font-size:0.67rem;color:#D4820A;margin-top:0.2rem;">{prem_pct:.0f}% of income — borderline</div>'
                else:
                    afford_text = f'<div style="font-size:0.67rem;color:#1A6B3C;margin-top:0.2rem;">{prem_pct:.0f}% of income — affordable</div>'

                # Payout context
                payout_text = "No payout if survives term" if payout == 0 else f"Payout: {fmt_coverage(payout/1000)}"

                # Survival timeline — place labels above bar to avoid overlap
                max_years = 60
                ytd_pct  = min(r["years_to_death"] / max_years * 100, 100)
                term_pct = min(r["policy_term"] / max_years * 100, 100) if r["policy_term"] > 0 else None

                labels = []
                if term_pct is not None:
                    labels.append(("term", term_pct, f"Term {r['policy_term']}yr", "#1B3A6B"))
                labels.append(("ytd", ytd_pct, f"✝ Est. death {r['years_to_death']:.1f}yr",  "#0F1C2E"))

                # Sort by position, then nudge overlapping labels
                labels.sort(key=lambda x: x[1])
                MIN_GAP = 12
                for i in range(1, len(labels)):
                    prev_pos = labels[i-1][1]
                    cur_pos  = labels[i][1]
                    if cur_pos - prev_pos < MIN_GAP:
                        labels[i] = (labels[i][0], prev_pos + MIN_GAP, labels[i][2], labels[i][3])

                above_labels = "".join(
                    f'<div style="position:absolute;left:{pos:.1f}%;transform:translateX(-50%);'
                    f'bottom:22px;font-size:0.62rem;font-weight:600;color:{lcolor};white-space:nowrap;">'
                    f'{ltext}</div>'
                    for _, pos, ltext, lcolor in labels
                )

                # Vertical markers
                markers = "".join(
                    f'<div style="position:absolute;left:{pos:.1f}%;top:-3px;bottom:-3px;'
                    f'width:2.5px;background:{lcolor};border-radius:2px;"></div>'
                    for _, pos, _, lcolor in labels
                )

                # Recommendation box
                rec_config = {
                    "ACCEPT":  ("🟢", "#1A6B3C", "#F0FDF4", "#BBF7D0", "Policy recommended for acceptance — projected profitable with affordable premium."),
                    "REFER":   ("🟡", "#92400E", "#FFFBEB", "#FDE68A", f"Refer for senior underwriter review — profitable but premium is {prem_pct:.0f}% of income."),
                    "DECLINE": ("🔴", "#C0392B", "#FEF2F2", "#FECACA", f"Policy recommended for decline — {'projected net loss' if not profitable else f'premium is {prem_pct:.0f}% of income and commercially unviable'}."),
                }
                rec_icon, rec_color, rec_bg, rec_border, rec_text = rec_config[rec]

                profit_html = f"""\
<div style="background:#fff;border:1px solid #D9E2EE;border-radius:10px;padding:1.4rem 1.6rem;margin-top:0.75rem;box-shadow:0 1px 3px rgba(15,28,46,0.07);">
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.1rem;padding-bottom:0.85rem;border-bottom:1px solid #F1F4F8;">
<div>
<div style="font-size:1.05rem;font-weight:700;color:#0F1C2E;">{r["name"]}</div>
<div style="font-size:0.75rem;color:#5B7290;margin-top:0.15rem;">Age {r["age"]:.0f} &nbsp;·&nbsp; {r["policy_type"]} &nbsp;·&nbsp; <span style="color:{tier_color};font-weight:600;">ML Score {r["risk_score"]}/20 · {r["risk_tier"]}</span></div>
</div>
<span style="padding:0.35rem 1.1rem;border-radius:999px;font-size:0.78rem;font-weight:700;background:{bg};color:{color};border:1.5px solid {border};">{label}</span>
</div>
<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:0.75rem;margin-bottom:1.4rem;">
<div style="background:#F8FAFC;border-radius:8px;padding:0.75rem;text-align:center;">
<div style="font-size:0.58rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.3rem;">Coverage</div>
<div style="font-size:1.6rem;font-weight:700;color:#0F1C2E;line-height:1;">{fmt_coverage(r["coverage"]/1000)}</div>
<div style="font-size:0.7rem;color:#5B7290;">{r["policy_type"]}</div>
</div>
<div style="background:#F8FAFC;border-radius:8px;padding:0.75rem;text-align:center;">
<div style="font-size:0.58rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.3rem;">Est. Life Remaining</div>
<div style="font-size:1.6rem;font-weight:700;color:#0F1C2E;line-height:1;">{r["years_to_death"]:.1f}</div>
<div style="font-size:0.7rem;color:#5B7290;">yrs · dies age {r["age"]+r["years_to_death"]:.0f}</div>
</div>
<div style="background:#F8FAFC;border-radius:8px;padding:0.75rem;text-align:center;">
<div style="font-size:0.58rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.3rem;">Annual Premium</div>
<div style="font-size:1.6rem;font-weight:700;color:#1B3A6B;line-height:1;">${r["annual_premium"]:,.0f}</div>
{afford_text}
</div>
<div style="background:#F8FAFC;border-radius:8px;padding:0.75rem;text-align:center;">
<div style="font-size:0.58rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.3rem;">Profit Margin</div>
<div style="font-size:1.6rem;font-weight:700;color:{'#1A6B3C' if r['profit_margin'] >= 0 else '#C0392B'};line-height:1;">{r['profit_margin']:.0f}%</div>
<div style="font-size:0.7rem;color:#5B7290;">{'net gain on premiums' if r['profit_margin'] >= 0 else 'net loss on premiums'}</div>
</div>
<div style="background:#F0FDF4;border-radius:8px;padding:0.75rem;text-align:center;border:1px solid #BBF7D0;">
<div style="font-size:0.58rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.3rem;">Premiums Collected</div>
<div style="font-size:1.6rem;font-weight:700;color:#1A6B3C;line-height:1;">{fmt_coverage(r["total_premiums"]/1000)}</div>
<div style="font-size:0.7rem;color:#5B7290;">over {r["years_paying"]:.0f} yrs</div>
</div>
<div style="background:#FEF2F2;border-radius:8px;padding:0.75rem;text-align:center;border:1px solid #FECACA;">
<div style="font-size:0.58rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.3rem;">Payout Liability</div>
<div style="font-size:1.6rem;font-weight:700;color:#C0392B;line-height:1;">{fmt_coverage(payout/1000) if payout > 0 else "—"}</div>
<div style="font-size:0.67rem;color:#5B7290;">{payout_text}</div>
</div>
</div>
<div style="display:flex;align-items:center;gap:1rem;margin-bottom:1.25rem;padding:0.65rem 1rem;background:#F8FAFC;border-radius:8px;border:1px solid #D9E2EE;">
<span style="font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;white-space:nowrap;">Net Outcome</span>
<span style="font-size:1.05rem;font-weight:700;color:{'#1A6B3C' if net >= 0 else '#C0392B'};">{'+ ' if net >= 0 else ''}{fmt_coverage(net/1000)}</span>
<span style="font-size:0.75rem;color:#5B7290;">= {fmt_coverage(r["total_premiums"]/1000)} premiums − {fmt_coverage(payout/1000) if payout > 0 else '$0'} payout</span>
</div>
<div style="font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:2rem;">Survival Timeline</div>
<div style="position:relative;height:16px;background:#F1F4F8;border-radius:99px;overflow:visible;margin-bottom:0.5rem;">
{above_labels}
<div style="position:absolute;left:0;top:0;width:{ytd_pct:.1f}%;height:16px;background:linear-gradient(to right,{'#A9DFBF,#D5F5E3' if profitable else '#F5B7B1,#FADBD8'});border-radius:99px;"></div>
{markers}
</div>
<div style="display:flex;justify-content:space-between;font-size:0.62rem;color:#8FA5BF;margin-top:0.4rem;">
<span>0yr</span><span>15yr</span><span>30yr</span><span>45yr</span><span>60yr</span>
</div>
<div style="margin-top:1.1rem;padding:0.85rem 1rem;background:{rec_bg};border-radius:8px;border:1px solid {rec_border};display:flex;align-items:flex-start;gap:0.75rem;">
<span style="font-size:1.1rem;flex-shrink:0;">{rec_icon}</span>
<div>
<div style="font-size:0.72rem;font-weight:700;color:{rec_color};text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.2rem;">{rec}</div>
<div style="font-size:0.82rem;color:#2C3E55;line-height:1.5;">{rec_text}</div>
</div>
</div>
</div>"""
                st.markdown(profit_html, unsafe_allow_html=True)

                breakdown_rows = [
                    ("Coverage", fmt_coverage(r['coverage']/1000), "Amount requested by applicant"),
                    ("÷ 1,000", f"{r['coverage']/1000:,.0f}", "Rate is per $1,000 of coverage"),
                    ("× Base rate", f"${r['_base_rate']:.3f}", f"Age-based: 0.5 × e^(0.055 × max({r['age']:.0f} − 25, 0))"),
                    ("× Risk multiplier", f"{r['_risk_mult']:.2f}×", f"1.0 + (ML score {r['risk_score']} ÷ 20)"),
                ]
                if r["_term_adj"] is not None:
                    breakdown_rows.append(("× Term adjustment", f"{r['_term_adj']:.2f}×", f"1.0 + ({r['policy_term']}yr − 10) × 0.02"))
                if r["_wl_mult"] is not None:
                    breakdown_rows.append(("× Whole Life factor", "8.00×", "Guaranteed payout → higher base cost"))
                breakdown_rows.append(("= Annual Premium", f"<strong>${r['annual_premium']:,.2f}</strong>", ""))
                trows = "".join(
                    f'<tr style="border-bottom:1px solid #F1F4F8;">'
                    f'<td style="padding:0.45rem 0.6rem;font-size:0.78rem;font-weight:600;color:#0F1C2E;white-space:nowrap;">{step}</td>'
                    f'<td style="padding:0.45rem 0.6rem;font-size:0.78rem;color:#1B3A6B;font-weight:700;text-align:right;white-space:nowrap;">{val}</td>'
                    f'<td style="padding:0.45rem 0.6rem;font-size:0.72rem;color:#5B7290;">{desc}</td>'
                    f'</tr>'
                    for step, val, desc in breakdown_rows
                )
                breakdown_html = (
                    f'<div style="margin-top:1rem;">'
                    f'<div style="font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.5rem;">Premium Calculation Breakdown</div>'
                    f'<table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #D9E2EE;border-radius:8px;overflow:hidden;">'
                    f'<thead><tr style="background:#F8FAFC;border-bottom:2px solid #D9E2EE;">'
                    f'<th style="padding:0.5rem 0.6rem;font-size:0.62rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;text-align:left;">Step</th>'
                    f'<th style="padding:0.5rem 0.6rem;font-size:0.62rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;text-align:right;">Value</th>'
                    f'<th style="padding:0.5rem 0.6rem;font-size:0.62rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;text-align:left;">Logic</th>'
                    f'</tr></thead><tbody>{trows}</tbody></table></div>'
                )
                st.markdown(breakdown_html, unsafe_allow_html=True)

                legend_html = (
                    '<div style="margin-top:0.75rem;padding:0.85rem 1rem;background:#F8FAFC;border:1px solid #E5E9F0;border-radius:8px;">'
                    '<div style="font-size:0.62rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:#5B7290;margin-bottom:0.6rem;">Formula Reference</div>'
                    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:0.4rem 1.5rem;">'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">0.5</strong> — base cost coefficient ($0.50 per $1,000 at age 25, the starting reference age)'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">0.055</strong> — annual mortality acceleration factor. Mortality risk grows ~5.5% per year of age'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">25</strong> — reference age. Below 25, base rate is flat at $0.50; above, it grows exponentially'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">÷ 20</strong> — normalizes the ML risk score (0–20) into a 0.0–1.0 multiplier range'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">1.0 +</strong> — ensures even a score-0 applicant pays at least the base rate (1.0×), not zero'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">0.02</strong> — term length surcharge. Each year of term beyond 10 adds 2% to premium (longer exposure = more risk)'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">10</strong> — baseline term. A 10-year policy has no surcharge (1.0×); 20yr = 1.2×, 30yr = 1.4×'
                    '</div>'
                    '<div style="font-size:0.73rem;color:#2C3E55;line-height:1.6;">'
                    '<strong style="color:#1B3A6B;">8.00×</strong> — Whole Life multiplier. Payout is guaranteed (vs conditional in Term), so base cost is 8× higher'
                    '</div>'
                    '</div>'
                    '</div>'
                )
                st.markdown(legend_html, unsafe_allow_html=True)

        st.caption("⚠️ Actuarial estimates based on US life table multipliers. For illustrative purposes only.")



# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Ask Your Data  (Cortex Analyst)
# ═══════════════════════════════════════════════════════════════════════════════

ANALYST_ENDPOINT = "/api/v2/cortex/analyst/message"
SEMANTIC_MODEL   = "@UNDERWRITING_DB.UNDERWRITING_SCHEMA.YAML_STAGE/underwriting_model.yaml"


def call_analyst(messages: list[dict]) -> dict:
    payload = {
        "messages": messages,
        "semantic_model_file": SEMANTIC_MODEL,
    }
    response = _snowflake.send_snow_api_request(
        "POST", ANALYST_ENDPOINT, {}, {}, payload, {}, 180000
    )
    status = response.get("status", 500)
    raw    = response.get("content", "")

    if status >= 400:
        return {"error": f"HTTP {status}: {str(raw)[:400]}"}

    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return {"error": f"Invalid response: {str(raw)[:400]}"}

    text_parts = []
    sql_found  = None

    message = raw.get("message", raw)
    content_items = message.get("content", []) if isinstance(message, dict) else []

    for item in content_items:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text":
            text_parts.append(item.get("text", ""))
        elif item.get("type") == "sql":
            sql_found = item.get("statement", "")

    return {
        "text": "\n".join(text_parts).strip(),
        "sql":  sql_found,
        "raw":  raw,
    }


def get_portfolio_summary() -> str:
    try:
        df = session.sql(f"""
            SELECT FULL_LEGAL_NAME, AGE, BMI_NUMERIC, SYSTOLIC_BP, DIASTOLIC_BP,
                   CHOLESTEROL_NUMERIC, GLUCOSE_NUMERIC, ANNUAL_INCOME,
                   COVERAGE_AMOUNT_NUMERIC, SMOKING_STATUS, PRE_EXISTING_CONDITIONS,
                   RISK_SCORE, RISK_TIER, UNDERWRITER_SUMMARY
            FROM {TABLE_FQN}
            ORDER BY RISK_SCORE DESC
        """).to_pandas()
        if df.empty:
            return "No applicants in the portfolio yet."
        return df.to_csv(index=False)
    except Exception:
        return "Could not fetch portfolio data."


def generate_insight(question: str, data_csv: str | None, analyst_text: str | None) -> str:
    context = ""
    if data_csv:
        context = f"\n\nDATA:\n{data_csv[:6000]}"
    if analyst_text:
        context += f"\n\nANALYST NOTE:\n{analyst_text}"

    prompt_text = f"""You are a senior life insurance underwriting consultant.
You have access to the applicant portfolio data below.
Answer the user's question with specific data references, names, and numbers.
Be decisive — give clear recommendations with reasoning.
Use professional but conversational tone. Be concise.

Underwriting guidelines:
- RISK_SCORE 0-6 = LOW risk (generally acceptable)
- RISK_SCORE 7-13 = MEDIUM risk (review needed, may require conditions)
- RISK_SCORE 14-20 = HIGH risk (likely decline or heavy loading)
- Coverage/income ratio above 10x is a concern
- Active smokers, prior declines, and serious pre-existing conditions are key red flags
{context}

USER QUESTION: {question}"""

    escaped = prompt_text.replace("'", "''")
    try:
        rows = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped}') AS answer
        """).collect()
        if rows:
            return str(rows[0]["ANSWER"]).strip()
    except Exception as e:
        return f"Could not generate insight: {e}"
    return ""


with tab_chat:
    st.markdown("""
    <div class="chat-header">
        <div class="ch-pulse"></div>
        <div class="ch-title">Underwriting Analyst</div>
        <div class="ch-sub">Powered by Snowflake Cortex Analyst</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Suggested questions**")

    sug_cols    = st.columns(3)
    suggestions = [
        "How many high-risk applicants do we have?",
        "What is the average risk score and why?",
        "Based on the current portfolio, provide a strategic recommendation on which applicants should be accepted, declined or referred for further review.",
    ]
    for col, suggestion in zip(sug_cols, suggestions):
        with col:
            if st.button(suggestion, use_container_width=True, key=f"sug_{suggestion[:18]}"):
                st.session_state["_pending_prompt"] = suggestion

    st.divider()

    if "messages" not in st.session_state: st.session_state.messages = []

    for msg in st.session_state.messages:
        role = msg.get("role", "user")
        with st.chat_message(role):
            content_items = msg.get("content", [])
            if isinstance(content_items, str):
                st.write(content_items)
            elif isinstance(content_items, list):
                for item in content_items:
                    if isinstance(item, dict):
                        if item.get("type") == "text":
                            st.write(item.get("text", ""))
                        elif item.get("type") == "sql":
                            with st.expander("View generated SQL"):
                                st.code(item.get("statement", ""), language="sql")
            if msg.get("_df") is not None:
                st.dataframe(msg["_df"], hide_index=True, use_container_width=True)

    prompt        = st.chat_input("Ask a question about your portfolio…")
    active_prompt = prompt or st.session_state.pop("_pending_prompt", None)

    if active_prompt:
        with st.chat_message("user"):
            st.write(active_prompt)

        st.session_state.messages.append({"role": "user", "content": [{"type": "text", "text": active_prompt}]})

        api_messages = [{"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                        if m["role"] in ("user", "analyst")]

        with st.chat_message("analyst"):
            with st.spinner("Querying Cortex Analyst…"):
                try:
                    result = call_analyst(api_messages)

                    if "error" in result:
                        st.error(result["error"])
                        st.session_state.messages.append({
                            "role": "analyst",
                            "content": [{"type": "text", "text": result["error"]}]
                        })
                    else:
                        resp_content = []
                        df = None

                        if result["sql"]:
                            with st.expander("View generated SQL"):
                                st.code(result["sql"], language="sql")
                            try:
                                df = session.sql(result["sql"]).to_pandas()
                            except Exception as sql_err:
                                st.warning(f"Could not execute SQL: {sql_err}")

                        analyst_couldnt_answer = (
                            not result["sql"] and result["text"] and
                            any(w in result["text"].lower()
                                for w in ["cannot", "sorry", "unclear", "clarify"])
                        )

                        data_csv = None
                        if df is not None and not df.empty:
                            data_csv = df.to_csv(index=False)
                        elif analyst_couldnt_answer:
                            data_csv = get_portfolio_summary()

                        if data_csv or analyst_couldnt_answer:
                            with st.spinner("Generating insight…"):
                                insight = generate_insight(
                                    active_prompt, data_csv,
                                    result["text"] if not analyst_couldnt_answer else None
                                )
                            if insight:
                                st.markdown(insight)
                                resp_content.append({"type": "text", "text": insight})
                        elif result["text"]:
                            st.write(result["text"])
                            resp_content.append({"type": "text", "text": result["text"]})

                        if result["sql"]:
                            resp_content.append({"type": "sql", "statement": result["sql"]})

                        msg_entry = {"role": "analyst", "content": resp_content, "_df": None}

                        if df is not None and not df.empty:
                            msg_entry["_df"] = df
                            st.dataframe(df, hide_index=True, use_container_width=True)
                            try:
                                import pandas as pd
                                num_cols = df.select_dtypes(include="number").columns.tolist()
                                cat_cols = df.select_dtypes(exclude="number").columns.tolist()
                                if len(num_cols) >= 1 and len(df) > 1:
                                    chart_col = num_cols[0]
                                    idx_col   = cat_cols[0] if cat_cols else df.columns[0]
                                    st.bar_chart(df.set_index(idx_col)[chart_col])
                            except Exception:
                                pass

                        if not resp_content:
                            st.info("No results returned.")
                            msg_entry["content"] = [{"type": "text", "text": "No results returned."}]

                        st.session_state.messages.append(msg_entry)

                except Exception as e:
                    st.error(f"Unexpected error: {e}")
                    st.session_state.messages.append({
                        "role": "analyst",
                        "content": [{"type": "text", "text": f"Error: {e}"}]
                    })

    if st.session_state.get("messages"):
        if st.button("Clear conversation", key="clear_chat"):
            st.session_state.messages = []
            st.rerun()
