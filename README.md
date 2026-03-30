# NamUS Missing Persons — Data Science Project
**COMP30780 | University College Dublin | 2025–26**

An analysis of missing persons cases in the United States, combining data from the National Missing and Unidentified Persons System (NamUs) with county-level socioeconomic, geographic, and health indicators to investigate patterns in reporting, demographics, and case resolution.

---

## Research Questions

1. Do counties with higher poverty rates have disproportionately higher missing persons rates per capita?
2. Does socioeconomic vulnerability predict how long it takes for a case to be reported?
3. Is there a relationship between a county's racial demographic composition and the ethnicity of its missing persons cases — are certain groups underrepresented?
4. Do cases in more socioeconomically deprived areas have lower resolution rates?

---

## Project Structure

```
NamUS_project/
├── requirements.txt              # Python dependencies
├── data-processing/
│   ├── namus-scraper/
│   │   ├── scrape-data.py        # Fetches all missing persons cases from NamUs API
│   │   └── clean-data.py         # Flattens raw JSON into structured CSV
│   ├── enrich-data.py            # Joins external socioeconomic datasets (single year)
│   ├── historical-data.py        # Fetches multi-year snapshots and fits county-level trends
│   └── output/
│       ├── MissingPersons/       # Case-level data — NOT committed (personal information)
│       │   ├── MissingPersons.json            # Raw scraped data
│       │   ├── MissingPersons_clean.csv       # Cleaned case-level data (65 cols)
│       │   └── MissingPersons_enriched.csv    # Enriched with socioeconomic data (84 cols)
│       └── county-data/          # County-level data — committed (no personal info)
│           ├── county_socioeconomic.csv       # 2022 snapshot: ACS, SVI, Metro/Micro
│           ├── county_historical_long.csv     # Multi-year panel: 2012–2022 (35k rows)
│           └── county_trends.csv             # Per-county trend slopes, R², 2026 projections
└── project_reqs/                 # Module guidelines and templates
```

---

## Data Sources

| Dataset | Source | Level | Description |
|---|---|---|---|
| NamUs Missing Persons | namus.gov API | Case | 26,312 missing persons cases with demographics, location, circumstances |
| Census ACS 5-year (2022) | api.census.gov | County | Population, income, poverty, unemployment, education, race |
| CDC Social Vulnerability Index (2022) | svi.cdc.gov | County | Composite vulnerability score (0–1) |
| OMB CBSA Delineations (2023) | Census Bureau | County | Metro / Micropolitan / Rural classification |
| CDC PLACES (2023) | data.cdc.gov | County | % adults with poor mental health |
| FBI UCR (2019) | CORGIS / FBI | State | Violent and property crime rates per 100k |
| Census TIGER AIANNH (2022) | Census Bureau | Spatial | Tribal land boundaries — spatial join |

> **Note:** The `output/` directory is excluded from version control. Raw data contains personal information about real missing persons cases and must be handled responsibly. Run the pipeline below to reproduce the dataset locally.

---

## Reproducing the Dataset

### 1. Set up environment

```bash
python3 -m venv ~/namus-venv
source ~/namus-venv/bin/activate
pip install -r requirements.txt
```

> Note: If your project path contains a colon (e.g. `UCD 25:26`), create the venv outside the project directory as shown above.

### 2. Scrape missing persons data

```bash
cd data-processing
python3 namus-scraper/scrape-data.py
```

Fetches all ~26,000 missing persons cases from the NamUs API, searching per-state to bypass the 10,000-case search limit. Includes retry logic with delays to handle rate limiting. Output: `output/MissingPersons/MissingPersons.json`

### 3. Clean the data

```bash
python3 namus-scraper/clean-data.py
```

Flattens the nested JSON into a flat CSV with 65 columns. Derives fields including age at disappearance, days missing, reporting delay, and multi-state flags. Output: `output/MissingPersons/MissingPersons_clean.csv`

### 4. Enrich with socioeconomic data

```bash
python3 enrich-data.py
```

Fetches county and state-level data from 6 external sources and joins to the cleaned NamUs data by county FIPS code. Adds 19 new columns. Output: `output/MissingPersons/MissingPersons_enriched.csv` and `output/county-data/county_socioeconomic.csv`

### 5. Build historical trends

```bash
python3 historical-data.py
```

Fetches multi-year snapshots of county socioeconomic indicators (Census ACS 2012–2022, CDC SVI 2014–2022, FBI UCR 1960–2019) and fits per-county linear regressions to produce trend slopes, R², standard deviation of residuals, and 2026 projections. Outputs: `output/county-data/county_historical_long.csv` and `output/county-data/county_trends.csv`

---

## Ethical Considerations

- Data sourced from a US federal government public database (NamUs) intended for research and investigative use
- Contains real personal information (names, ages, locations) of missing persons — handled for research purposes only, not republished
- Raw data is excluded from version control
- No individual-level conclusions are drawn; analysis is statistical and aggregate
- Facial recognition scripts (available in the original NamUs scraper) are not used in this project

---

## Team

| Member | Responsibilities |
|---|---|
| Josie | Data pipeline, scraping, cleaning, enrichment |
| [Partner 2] | [RQs] |
| [Partner 3] | [RQs] |
