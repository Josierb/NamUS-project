"""
historical-data.py
Fetches multi-year snapshots of county-level socioeconomic indicators and
calculates per-county trends (slope, std dev of residuals, R², 2026 extrapolation).

Sources:
  - Census ACS 5-year estimates: 2009–2022 (api.census.gov)
  - CDC Social Vulnerability Index: 2010, 2014, 2016, 2018, 2020, 2022 (svi.cdc.gov)
  - FBI UCR crime rates: all available years 1960–2019 (CORGIS CSV mirror)

Outputs:
  - output/MissingPersons/county_historical_long.csv  (county × year × metrics)
  - output/MissingPersons/county_trends.csv           (slope, std, R², 2026 projection per county)
"""

import requests
import pandas as pd
import numpy as np
import io
from scipy import stats

LONG_OUTPUT   = "./output/county-data/county_historical_long.csv"
TRENDS_OUTPUT = "./output/county-data/county_trends.csv"

EXTRAPOLATE_YEAR = 2026

# ---------------------------------------------------------------------------
# Census ACS variables (consistent across years)
# ---------------------------------------------------------------------------
ACS_VARS = ",".join([
    "NAME",
    "B01003_001E",   # total population
    "B19013_001E",   # median household income
    "B17001_002E",   # pop below poverty
    "B17001_001E",   # total pop for poverty
    "B23025_005E",   # unemployed
    "B23025_002E",   # in labour force
    "B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E",  # degree holders
    "B15003_001E",   # total pop 25+ (education denominator)
])

# ACS 5-year estimates available from 2009 onward
ACS_YEARS = list(range(2009, 2023))

# CDC SVI available years and confirmed URL pattern
SVI_YEARS = [2010, 2014, 2016, 2018, 2020, 2022]
SVI_URL_PATTERN = "https://svi.cdc.gov/Documents/Data/{year}/csv/States_Counties/SVI_{year}_US_county.csv"

# CORGIS FBI dataset covers 1960–2019
FBI_URL = "https://corgis-edu.github.io/corgis/datasets/csv/state_crime/state_crime.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_fips(state_col, county_col):
    return state_col.astype(str).str.zfill(2) + county_col.astype(str).str.zfill(3)


def linreg_stats(years, values):
    """
    Fits a linear regression to (years, values) for a single county.
    Returns slope, intercept, r_squared, std_residuals, extrapolated value at EXTRAPOLATE_YEAR.
    Returns all NaN if fewer than 3 valid observations.
    """
    mask = ~np.isnan(values)
    if mask.sum() < 3:
        return np.nan, np.nan, np.nan, np.nan, np.nan

    x = years[mask]
    y = values[mask]
    slope, intercept, r, _, _ = stats.linregress(x, y)
    residuals = y - (slope * x + intercept)
    std_resid = np.std(residuals, ddof=1) if len(residuals) > 1 else np.nan
    projection = slope * EXTRAPOLATE_YEAR + intercept
    return slope, intercept, r ** 2, std_resid, projection


# ---------------------------------------------------------------------------
# Fetch ACS for a single year
# ---------------------------------------------------------------------------

def fetch_acs_year(year):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={ACS_VARS}&for=county:*&in=state:*"
    )
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        rows = r.json()
    except Exception as e:
        print(f"    WARNING: ACS {year} failed ({e})")
        return None

    df = pd.DataFrame(rows[1:], columns=rows[0])
    numeric_cols = [c for c in df.columns if c not in ("NAME", "state", "county")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df[numeric_cols] = df[numeric_cols].replace(-666666666, np.nan)

    df["fips"] = _to_fips(df["state"], df["county"])
    df["year"] = year

    df["poverty_rate"]      = df["B17001_002E"] / df["B17001_001E"] * 100
    df["unemployment_rate"] = df["B23025_005E"] / df["B23025_002E"] * 100
    df["pct_bachelors_plus"] = (
        df[["B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"]].sum(axis=1)
        / df["B15003_001E"] * 100
    )

    keep = [
        "fips", "year", "NAME",
        "B01003_001E", "B19013_001E",
        "poverty_rate", "unemployment_rate", "pct_bachelors_plus",
    ]
    df = df.rename(columns={
        "B01003_001E": "county_population",
        "B19013_001E": "median_household_income",
    })
    return df[["fips", "year", "NAME", "county_population", "median_household_income",
               "poverty_rate", "unemployment_rate", "pct_bachelors_plus"]]


# ---------------------------------------------------------------------------
# Fetch SVI for a single year
# ---------------------------------------------------------------------------

def fetch_svi_year(year):
    url = SVI_URL_PATTERN.format(year=year)
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig")))
    except Exception as e:
        print(f"    WARNING: SVI {year} failed ({e})")
        return None

    # FIPS column name varies across years
    fips_col = next((c for c in df.columns if c.upper() in ("FIPS", "STCOFIPS")), None)
    svi_col  = next((c for c in df.columns if c.upper() == "RPL_THEMES"), None)

    if not fips_col or not svi_col:
        print(f"    WARNING: SVI {year} — could not find FIPS or RPL_THEMES column. Columns: {list(df.columns[:10])}")
        return None

    df = df.rename(columns={fips_col: "fips", svi_col: "svi_score"})
    df["fips"] = df["fips"].astype(str).str.zfill(5)
    df["svi_score"] = pd.to_numeric(df["svi_score"], errors="coerce")
    df.loc[df["svi_score"] < 0, "svi_score"] = np.nan
    df["year"] = year
    return df[["fips", "year", "svi_score"]].dropna(subset=["fips"])


# ---------------------------------------------------------------------------
# Fetch FBI UCR (all years)
# ---------------------------------------------------------------------------

def fetch_fbi_all_years():
    print("  Fetching FBI UCR crime rates (all years)...")
    try:
        r = requests.get(FBI_URL, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df = df.rename(columns={
            "State": "state",
            "Year":  "year",
            "Data.Rates.Violent.All":  "violent_crime_rate_per_100k",
            "Data.Rates.Property.All": "property_crime_rate_per_100k",
        })
        df["state"] = df["state"].str.lower().str.strip()
        print(f"    {len(df)} state-year rows fetched ({df['year'].min()}–{df['year'].max()})")
        return df[["state", "year", "violent_crime_rate_per_100k", "property_crime_rate_per_100k"]]
    except Exception as e:
        print(f"    WARNING: FBI data failed ({e})")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # -----------------------------------------------------------------------
    # 1. Fetch ACS across years
    # -----------------------------------------------------------------------
    print(f"Fetching Census ACS 5-year estimates for {ACS_YEARS[0]}–{ACS_YEARS[-1]}...")
    acs_frames = []
    for year in ACS_YEARS:
        print(f"  ACS {year}...", end=" ", flush=True)
        df = fetch_acs_year(year)
        if df is not None:
            acs_frames.append(df)
            print(f"{len(df)} counties")
        else:
            print("skipped")

    acs_long = pd.concat(acs_frames, ignore_index=True) if acs_frames else pd.DataFrame()
    print(f"  Total ACS rows: {len(acs_long)}\n")

    # -----------------------------------------------------------------------
    # 2. Fetch SVI across years
    # -----------------------------------------------------------------------
    print(f"Fetching CDC SVI for years: {SVI_YEARS}...")
    svi_frames = []
    for year in SVI_YEARS:
        print(f"  SVI {year}...", end=" ", flush=True)
        df = fetch_svi_year(year)
        if df is not None:
            svi_frames.append(df)
            print(f"{len(df)} counties")
        else:
            print("skipped")

    svi_long = pd.concat(svi_frames, ignore_index=True) if svi_frames else pd.DataFrame()
    print(f"  Total SVI rows: {len(svi_long)}\n")

    # -----------------------------------------------------------------------
    # 3. Fetch FBI (state-level, all years)
    # -----------------------------------------------------------------------
    fbi_all = fetch_fbi_all_years()
    print()

    # -----------------------------------------------------------------------
    # 4. Build county long-format table
    # -----------------------------------------------------------------------
    print("Building county historical long table...")

    # ACS is the backbone; left-join SVI on fips+year
    if not acs_long.empty and not svi_long.empty:
        county_long = acs_long.merge(svi_long, on=["fips", "year"], how="left")
    elif not acs_long.empty:
        county_long = acs_long.copy()
    else:
        county_long = svi_long.copy()

    # Parse county/state names from the ACS NAME field ("County Name, State Name")
    if "NAME" in county_long.columns:
        split = county_long["NAME"].str.rsplit(", ", n=1, expand=True)
        county_long["county_name"] = split[0]
        county_long["state_name"]  = split[1] if split.shape[1] > 1 else pd.NA

    # Attach FBI state-level data via state name
    if fbi_all is not None and "state_name" in county_long.columns:
        county_long["state_lower"] = county_long["state_name"].str.lower().str.strip()
        county_long = county_long.merge(
            fbi_all, left_on=["state_lower", "year"], right_on=["state", "year"], how="left"
        )
        county_long = county_long.drop(columns=["state_lower", "state"], errors="ignore")

    # Clean up column order
    id_cols = ["fips", "year", "county_name", "state_name"]
    id_cols = [c for c in id_cols if c in county_long.columns]
    metric_cols = [c for c in county_long.columns if c not in id_cols + ["NAME"]]
    county_long = county_long[id_cols + metric_cols]

    print(f"  {len(county_long)} county-year rows, {len(county_long.columns)} columns")
    county_long.to_csv(LONG_OUTPUT, index=False)
    print(f"  Saved to {LONG_OUTPUT}\n")

    # -----------------------------------------------------------------------
    # 5. Fit per-county trends
    # -----------------------------------------------------------------------
    print("Fitting per-county linear trends...")

    trend_metrics = [
        "county_population",
        "median_household_income",
        "poverty_rate",
        "unemployment_rate",
        "pct_bachelors_plus",
        "svi_score",
        "violent_crime_rate_per_100k",
        "property_crime_rate_per_100k",
    ]
    trend_metrics = [m for m in trend_metrics if m in county_long.columns]

    # Group by FIPS
    fips_groups = county_long.groupby("fips")

    trend_rows = []
    for fips, group in fips_groups:
        group = group.sort_values("year")
        years_arr = group["year"].to_numpy(dtype=float)

        row = {"fips": fips}
        # Carry forward county/state name (most recent non-null)
        if "county_name" in group.columns:
            row["county_name"] = group["county_name"].dropna().iloc[-1] if group["county_name"].notna().any() else np.nan
        if "state_name" in group.columns:
            row["state_name"] = group["state_name"].dropna().iloc[-1] if group["state_name"].notna().any() else np.nan

        for metric in trend_metrics:
            if metric not in group.columns:
                continue
            vals = group[metric].to_numpy(dtype=float)
            slope, intercept, r2, std_resid, proj = linreg_stats(years_arr, vals)
            # Snapshot at most recent year
            latest_val = group[metric].dropna().iloc[-1] if group[metric].notna().any() else np.nan
            latest_year = group.loc[group[metric].notna(), "year"].iloc[-1] if group[metric].notna().any() else np.nan

            row[f"{metric}_latest"]       = latest_val
            row[f"{metric}_latest_year"]  = latest_year
            row[f"{metric}_slope"]        = slope
            row[f"{metric}_r2"]           = r2
            row[f"{metric}_std_resid"]    = std_resid
            row[f"{metric}_{EXTRAPOLATE_YEAR}"] = proj

        trend_rows.append(row)

    trends = pd.DataFrame(trend_rows)
    print(f"  {len(trends)} counties with trend data")
    trends.to_csv(TRENDS_OUTPUT, index=False)
    print(f"  Saved to {TRENDS_OUTPUT}")

    # -----------------------------------------------------------------------
    # 6. Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"TREND SUMMARY  (n={len(trends)} counties)")
    print(f"{'='*60}")
    print(f"{'Metric':<35} {'Median slope':>14} {'Median R²':>10} {'Coverage':>10}")
    print("-" * 72)
    for metric in trend_metrics:
        slope_col = f"{metric}_slope"
        r2_col    = f"{metric}_r2"
        if slope_col not in trends.columns:
            continue
        n_valid  = trends[slope_col].notna().sum()
        pct      = n_valid / len(trends) * 100
        med_s    = trends[slope_col].median()
        med_r2   = trends[r2_col].median()
        print(f"  {metric:<33} {med_s:>14.4f} {med_r2:>10.3f} {pct:>9.1f}%")

    print("\nDone.")


main()
