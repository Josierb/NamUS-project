"""
enrich-data.py
Fetches county/state-level socioeconomic data from:
  - US Census Bureau ACS 5-year estimates (2022)
  - CDC Social Vulnerability Index (2022)
  - USDA Rural-Urban Continuum Codes (2023)
  - CDC PLACES Mental Health indicators (2023)
  - FBI UCR Violent Crime rates by state (2019, most recent bulk release)
  - Census TIGER Tribal Land boundaries — spatial join (BIA/AIANNH)
Joins to cleaned NamUS missing persons data by state + county.
Outputs a new enriched CSV, leaving the original untouched.
"""

import requests
import pandas as pd
import io
import zipfile
import tempfile
import os
import geopandas as gpd
from shapely.geometry import Point

CLEAN_INPUT     = "./output/MissingPersons/MissingPersons_clean.csv"
ENRICHED_OUTPUT = "./output/MissingPersons/MissingPersons_enriched.csv"
COUNTY_OUTPUT   = "./output/MissingPersons/county_socioeconomic.csv"

# ---------------------------------------------------------------------------
# Census ACS variable reference:
#   B01003_001E  Total population
#   B19013_001E  Median household income
#   B17001_002E  Population below poverty level
#   B17001_001E  Total pop for poverty determination
#   B23025_005E  Unemployed (civilian labour force)
#   B23025_002E  In labour force
#   B15003_022E-025E  Bachelor's/Master's/Professional/Doctorate
#   B15003_001E  Total pop 25+ (education denominator)
#   B02001_002E-008E  Race categories
# ---------------------------------------------------------------------------

ACS_VARS = ",".join([
    "NAME",
    "B01003_001E",
    "B19013_001E",
    "B17001_002E", "B17001_001E",
    "B23025_005E", "B23025_002E",
    "B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E", "B15003_001E",
    "B02001_002E", "B02001_003E", "B02001_004E",
    "B02001_005E", "B02001_006E", "B02001_007E", "B02001_008E",
])

ACS_URL = (
    f"https://api.census.gov/data/2022/acs/acs5"
    f"?get={ACS_VARS}&for=county:*&in=state:*"
)


def fetch_acs():
    print("Fetching Census ACS 5-year (2022) county data...")
    r = requests.get(ACS_URL, timeout=60)
    r.raise_for_status()
    rows = r.json()
    df = pd.DataFrame(rows[1:], columns=rows[0])

    numeric_cols = [c for c in df.columns if c not in ("NAME", "state", "county")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df[numeric_cols] = df[numeric_cols].replace(-666666666, pd.NA)

    df["poverty_rate"]       = df["B17001_002E"] / df["B17001_001E"] * 100
    df["unemployment_rate"]  = df["B23025_005E"] / df["B23025_002E"] * 100
    df["pct_bachelors_plus"] = (
        df[["B15003_022E","B15003_023E","B15003_024E","B15003_025E"]].sum(axis=1)
        / df["B15003_001E"] * 100
    )
    df["pct_white"]  = df["B02001_002E"] / df["B01003_001E"] * 100
    df["pct_black"]  = df["B02001_003E"] / df["B01003_001E"] * 100
    df["pct_native"] = df["B02001_004E"] / df["B01003_001E"] * 100
    df["pct_asian"]  = df["B02001_005E"] / df["B01003_001E"] * 100

    df["fips"] = df["state"] + df["county"]
    df[["acs_county_raw", "acs_state_raw"]] = df["NAME"].str.rsplit(", ", n=1, expand=True)
    df = df.rename(columns={
        "B01003_001E": "county_population",
        "B19013_001E": "median_household_income",
    })

    keep = [
        "fips", "acs_county_raw", "acs_state_raw",
        "county_population", "median_household_income",
        "poverty_rate", "unemployment_rate", "pct_bachelors_plus",
        "pct_white", "pct_black", "pct_native", "pct_asian",
    ]
    print(f"  {len(df)} counties fetched")
    return df[keep]


def fetch_svi():
    print("Fetching CDC Social Vulnerability Index (2022)...")
    try:
        url = "https://svi.cdc.gov/Documents/Data/2022/csv/States_Counties/SVI_2022_US_county.csv"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig")))
        df = df.rename(columns={"FIPS": "fips", "RPL_THEMES": "svi_score"})
        df["svi_score"] = pd.to_numeric(df["svi_score"], errors="coerce")
        df.loc[df["svi_score"] < 0, "svi_score"] = pd.NA
        df["fips"] = df["fips"].astype(str).str.zfill(5)
        print(f"  {len(df)} counties fetched")
        return df[["fips", "svi_score"]]
    except Exception as e:
        print(f"  WARNING: Could not fetch SVI ({e}). Skipping.")
        return None


def fetch_rural_urban():
    """OMB Core Based Statistical Area (CBSA) delineations (2023).
    Classifies every county as Metropolitan, Micropolitan, or Rural.
    Source: US Census Bureau delineation files."""
    print("Fetching OMB Metro/Micro delineation codes (2023)...")
    try:
        url = (
            "https://www2.census.gov/programs-surveys/metro-micro/geographies/"
            "reference-files/2023/delineation-files/list1_2023.xlsx"
        )
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content), header=2, dtype=str)

        # Build FIPS from state + county codes
        df["fips"] = df["FIPS State Code"].str.zfill(2) + df["FIPS County Code"].str.zfill(3)

        # Classify: Metropolitan / Micropolitan / Rural
        df = df.rename(columns={
            "Metropolitan/Micropolitan Statistical Area": "metro_micro_type",
            "CBSA Title": "cbsa_title",
        })
        df["urban_rural"] = df["metro_micro_type"].map({
            "Metropolitan Statistical Area": "Metro",
            "Micropolitan Statistical Area": "Micro",
        }).fillna("Rural")

        df = df.dropna(subset=["fips"]).drop_duplicates("fips")
        print(f"  {len(df)} counties classified")
        return df[["fips", "cbsa_title", "metro_micro_type", "urban_rural"]]
    except Exception as e:
        print(f"  WARNING: Could not fetch Metro/Micro codes ({e}). Skipping.")
        return None


def fetch_mental_health():
    """CDC PLACES 2023 — % adults with 14+ days poor mental health (county level)."""
    print("Fetching CDC PLACES Mental Health indicators (2023)...")
    try:
        url = (
            "https://data.cdc.gov/resource/swc5-untb.csv"
            "?measureid=MHLTH&$limit=5000"
        )
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))

        df = df.rename(columns={
            "locationid": "fips",
            "data_value": "pct_poor_mental_health",
        })
        df["fips"] = df["fips"].astype(str).str.zfill(5)
        df["pct_poor_mental_health"] = pd.to_numeric(df["pct_poor_mental_health"], errors="coerce")
        df = df.dropna(subset=["fips", "pct_poor_mental_health"])
        df = df.drop_duplicates("fips")
        print(f"  {len(df)} counties fetched")
        return df[["fips", "pct_poor_mental_health"]]
    except Exception as e:
        print(f"  WARNING: Could not fetch mental health data ({e}). Skipping.")
        return None


def fetch_fbi_crime():
    """
    FBI UCR state-level violent crime rate per 100k (2019).
    Note: 2019 is the most recent year available as a reliable bulk CSV.
    Source: CORGIS Dataset Project (mirrors FBI UCR).
    """
    print("Fetching FBI UCR violent crime rates by state (2019)...")
    try:
        url = "https://corgis-edu.github.io/corgis/datasets/csv/state_crime/state_crime.csv"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))

        # Filter to 2019 (most recent year in this dataset)
        df = df[df["Year"] == 2019].copy()
        df = df.rename(columns={"State": "state"})

        # Violent crime rate = violent crimes / population * 100,000
        df["violent_crime_rate_per_100k"]  = df["Data.Rates.Violent.All"]
        df["property_crime_rate_per_100k"] = df["Data.Rates.Property.All"]
        df["state"] = df["state"].str.lower().str.strip()
        print(f"  {len(df)} states fetched")
        return df[["state", "violent_crime_rate_per_100k", "property_crime_rate_per_100k"]]
    except Exception as e:
        print(f"  WARNING: Could not fetch FBI crime data ({e}). Skipping.")
        return None


def fetch_tribal_lands(namus_df):
    """
    Census TIGER AIANNH (American Indian/Alaska Native/Native Hawaiian areas).
    Spatial join: adds whether each case falls within a tribal land boundary.
    Source: US Census Bureau TIGER 2022 shapefiles.
    """
    print("Fetching Census TIGER tribal land boundaries (2022)...")
    try:
        url = "https://www2.census.gov/geo/tiger/TIGER2022/AIANNH/tl_2022_us_aiannh.zip"
        r = requests.get(url, timeout=120)
        r.raise_for_status()

        # Save zip to temp dir and read with geopandas
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "tribal.zip")
            with open(zip_path, "wb") as f:
                f.write(r.content)
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(tmpdir)
            shp_files = [f for f in os.listdir(tmpdir) if f.endswith(".shp")]
            tribal = gpd.read_file(os.path.join(tmpdir, shp_files[0]))

        tribal = tribal.to_crs("EPSG:4326")
        print(f"  {len(tribal)} tribal land areas loaded")

        # Build GeoDataFrame from NamUS lat/lon
        valid = namus_df[namus_df["lat"].notna() & namus_df["lon"].notna()].copy()
        gdf = gpd.GeoDataFrame(
            valid,
            geometry=gpd.points_from_xy(valid["lon"], valid["lat"]),
            crs="EPSG:4326",
        )

        # Spatial join — find which cases fall inside a tribal boundary
        joined = gpd.sjoin(gdf, tribal[["geometry", "NAMELSAD"]], how="left", predicate="within")
        joined["within_tribal_boundary"] = joined["NAMELSAD"].notna()
        joined = joined.rename(columns={"NAMELSAD": "tribal_land_name"})

        # Handle duplicates (case in multiple areas — keep first match)
        joined = joined[~joined.index.duplicated(keep="first")]

        result = joined[["within_tribal_boundary", "tribal_land_name"]].reindex(namus_df.index)
        result["within_tribal_boundary"] = result["within_tribal_boundary"].fillna(False)

        in_tribal = result["within_tribal_boundary"].sum()
        print(f"  {in_tribal} cases fall within tribal land boundaries")
        return result
    except Exception as e:
        print(f"  WARNING: Could not fetch tribal land data ({e}). Skipping.")
        return None


def normalise_county(name):
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    for suffix in [" county", " parish", " borough", " census area",
                   " municipality", " city and borough"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return name.strip()


def normalise_state(name):
    if not isinstance(name, str):
        return ""
    return name.lower().strip()


def build_join_key(county, state):
    return normalise_county(county) + "||" + normalise_state(state)


def main():
    print("Loading cleaned NamUS data...")
    namus = pd.read_csv(CLEAN_INPUT)
    print(f"  {len(namus)} cases loaded\n")

    # --- Fetch all sources ---
    acs        = fetch_acs()
    svi        = fetch_svi()
    rucc       = fetch_rural_urban()
    mental     = fetch_mental_health()
    fbi        = fetch_fbi_crime()
    tribal     = fetch_tribal_lands(namus)

    print()

    # --- Build join keys ---
    acs["join_key"] = acs.apply(
        lambda r: build_join_key(r["acs_county_raw"], r["acs_state_raw"]), axis=1
    )
    acs = acs.drop_duplicates("join_key")

    namus["join_key"] = namus.apply(
        lambda r: build_join_key(r.get("county"), r.get("state")), axis=1
    )

    # --- Join county-level data (ACS → then FIPS-keyed) ---
    print("Joining data to NamUS cases...")
    enriched = namus.merge(
        acs.drop(columns=["acs_county_raw", "acs_state_raw"]),
        on="join_key", how="left",
    )
    for df, label in [(svi, "SVI"), (rucc, "Rural-Urban"), (mental, "Mental Health")]:
        if df is not None:
            enriched = enriched.merge(df, on="fips", how="left")
            print(f"  Joined {label}")

    enriched = enriched.drop(columns=["join_key"])

    # --- Join state-level FBI data ---
    if fbi is not None:
        enriched["state_lower"] = enriched["state"].str.lower().str.strip()
        enriched = enriched.merge(fbi, left_on="state_lower", right_on="state", how="left", suffixes=("", "_fbi"))
        enriched = enriched.drop(columns=["state_lower", "state_fbi"], errors="ignore")
        print("  Joined FBI Crime")

    # --- Attach tribal spatial join ---
    if tribal is not None:
        enriched = enriched.join(tribal)
        print("  Joined Tribal Boundaries")

    # --- Coverage report ---
    original_cols = set(pd.read_csv(CLEAN_INPUT, nrows=0).columns)
    new_cols = [c for c in enriched.columns if c not in original_cols]
    print(f"\nNew columns added: {len(new_cols)}")
    print(f"  {'Column':<35} {'Non-null':>10} {'Coverage':>10}")
    print("  " + "-" * 58)
    for col in new_cols:
        nn = enriched[col].notna().sum()
        pct = nn / len(enriched) * 100
        print(f"  {col:<35} {nn:>10} {pct:>9.1f}%")

    # --- Save enriched case-level CSV ---
    print(f"\nWriting {ENRICHED_OUTPUT}...")
    enriched.to_csv(ENRICHED_OUTPUT, index=False)
    print(f"  {len(enriched)} rows, {len(enriched.columns)} columns")

    # --- Save standalone county-level CSV ---
    county_df = acs.drop(columns=["join_key"], errors="ignore").rename(
        columns={"acs_county_raw": "county", "acs_state_raw": "state"}
    )
    for df in [svi, rucc, mental]:
        if df is not None:
            county_df = county_df.merge(df, on="fips", how="left")
    print(f"\nWriting {COUNTY_OUTPUT}...")
    county_df.to_csv(COUNTY_OUTPUT, index=False)
    print(f"  {len(county_df)} counties")

    print("\nDone.")


main()
