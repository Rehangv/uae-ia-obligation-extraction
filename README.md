# UAE IA Regulation — Obligation Extraction & Gap Analysis System

An automated pipeline that reads the **UAE Information Assurance Regulation PDF**, extracts all compliance obligations using Google Gemini AI, and outputs a structured Excel file. Includes a **Gap Analysis** tool to compare two versions of the regulation and identify what changed.

---

## What It Does

- Parses the UAE IA Regulation PDF — detects chapters, sections, control families (M1–M6, T1–T9), and tables
- Extracts compliance obligations (what entities **shall** or **must** do) using Gemini AI
- Classifies obligations as: Procedural Requirement / Filing & Return / Display
- Outputs a clean, styled Excel with all obligations per section
- Compares two regulation versions and produces a colour-coded Gap Analysis Excel

---

## Project Structure

```
obligation code/
└── src/
    ├── app.py                   # Entry point — run this to process PDFs
    ├── duality.py               # Orchestrator — walks input folder, calls parser + AI
    ├── duality_obligation.py    # Alternative entry — re-run AI on existing Excel
    ├── RBI_code_7_loop.py       # PDF parser — extracts text, tables, page numbers
    ├── obligations_new_code.py  # AI pipeline — sends text to Gemini, post-processes output
    ├── gap_analysis.py          # Gap Analysis — compares two regulation Excel outputs
    ├── excel_styling.py         # Shared Excel styling — colours, fonts, borders
    ├── rbi_constants.py         # Column name constants used across all files
    ├── status_code.py           # Standardised status codes for error handling
    ├── Clean_obg.py             # Utility — splits obligations on Roman numerals
    ├── Regulations.xlsx         # Reference mapping file
    ├── log.txt                  # Runtime log output
    ├── input/                   # Drop your PDF files here
    └── output/                  # Generated Excel files appear here
```

---

## Requirements

- Python 3.10+
- Java 8+ (required by tabula-py for table extraction)
  - Download: https://www.java.com/en/download/
  - Verify: `java -version`

---

## Installation

**1. Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
cd YOUR_REPO_NAME/src
```

**2. Create and activate a virtual environment**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac / Linux
source venv/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Add your Google Gemini API key**

Open `obligations_new_code.py` and find the `GOOGLE_API_KEYS` list near the top. Add your key:
```python
GOOGLE_API_KEYS = [
    "your-gemini-api-key-here",
]
```

Do the same in `gap_analysis.py` — find `GAP_API_KEYS` and add your key.

> Get a free Gemini API key at: https://aistudio.google.com/app/apikey

---

## How to Run

### Extract Obligations from a PDF

1. Place your PDF file inside the `input/` folder
2. Run:
```bash
python app.py
```
3. Output Excel appears in `output/<pdf_name>/`

---

### Run Gap Analysis (Compare Two Versions)

Open `gap_analysis.py` and set the file paths at the bottom of the file:
```python
EXCEL1_PATH = r"output\UAE IA Regulation v11\UAE IA Regulation v11.xlsx"
EXCEL2_PATH = r"output\Modified_UAE_Circular\Modified_UAE_Circular.xlsx"
OUTPUT_PATH = r"output\Gap_Analysis.xlsx"
```

Then run:
```bash
python gap_analysis.py
```

Output: `Gap_Analysis.xlsx` with colour-coded results:

| Colour | Meaning |
|--------|---------|
| White  | No Change |
| Amber  | Obligation changed between versions |
| Green  | New section in Version 2 |
| Grey   | Section removed in Version 2 |

---

## How It Works (Brief)

```
PDF dropped in input/
    ↓
app.py → duality.py
    ↓
RBI_code_7_loop.py
  - Skips cover page and TOC
  - Detects headings by font size
  - Maps footer page numbers
  - Extracts tables via tabula → tab.json
  - Saves intermediate Excel (regulatory text per section)
    ↓
obligations_new_code.py
  - Pre-filters rows (no "shall/must" → instant "No obligations.")
  - Combines regulatory text + matching table content by page number
  - Sends to Gemini AI in batches of 25
  - Deduplicates bullet points in AI response
  - Classifies and post-processes obligations
  - Saves final styled Excel
    ↓
gap_analysis.py (run separately)
  - Matches sections between two Excels by Section Number
  - Normalises text (plurals, articles, punctuation) before comparing
  - Only calls AI for sections with genuine differences
  - Outputs colour-coded Gap Analysis Excel
```

---

## Notes

- API keys are **not committed** to this repository. Add your own keys before running.
- `venv/` and `__pycache__/` are excluded via `.gitignore`.
- The `output/` folder is excluded from git — generated files stay local.
- tabula-py requires Java. If you see a Java error, install Java first.

---

## Original Codebase

This project was adapted from an RBI (Reserve Bank of India) circular processing pipeline and re-architected for UAE IA Regulation PDFs — different document structure, heading detection, table extraction, and pre-filtering logic.

---

## Obligation Detection — How It Works & Known Limitations

### Keywords the system looks for

Before calling the AI, every row is scanned for **mandatory language**. Only rows containing at least one of these words/phrases are sent to Gemini:

| Keyword | Example |
|---------|---------|
| `shall` | *"The entity shall establish a policy..."* |
| `must` | *"Controls must be implemented by..."* |
| `is required to` | *"The entity is required to submit..."* |
| `are required to` | *"Entities are required to begin..."* |
| `obliged to` | *"The entity is obliged to..."* |
| `will implement` | *"The entity will implement..."* |
| `needs to` | *"The entity needs to define..."* |

Rows with none of these get **"No obligations."** instantly — no AI call needed.

---

### What about "should"?

The UAE IA Regulation uses **"should"** exclusively inside **"Implementation Guidance (for information purpose only)"** sections — these are explicitly advisory and informational, not mandatory requirements. Skipping them is intentional and correct for this document.

However, if this pipeline is ever run on a **different regulation** where "should" carries mandatory weight, add it to the `MANDATORY_RE` pattern in `obligations_new_code.py`:

```python
MANDATORY_RE = re.compile(
    r'\b(shall|must|is required to|are required to|obliged to|will implement|needs to|should)\b',
    re.I
)
```

---

### Other obligation patterns not currently detected

These phrases can carry obligations in some regulations but are **not in the current pre-filter** because they do not appear as mandatory language in the UAE IA Regulation:

- `is responsible for`
- `is prohibited from`
- `is expected to`
- `it is mandatory that`
- `compliance requires`

If you adapt this pipeline for another regulation, review whether any of these need to be added.