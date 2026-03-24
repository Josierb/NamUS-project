import json
import pandas as pd
from datetime import datetime, date

INPUT_FILE = "./output/MissingPersons/MissingPersons.json"
OUTPUT_FILE = "./output/MissingPersons/MissingPersons_clean.csv"

TODAY = date.today()


def get(obj, *keys, default=None):
    """Safely traverse nested dicts."""
    for key in keys:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(key)
        if obj is None:
            return default
    return obj


def extract_case(case):
    # --- Core ---
    case_id         = case.get("id")
    created         = case.get("createdDateTime")
    modified        = case.get("modifiedDateTime")
    is_resolved     = case.get("caseIsResolved")
    has_image       = bool(case.get("images"))
    has_document    = bool(case.get("documents"))
    ncmec_number    = get(case, "caseIdentification", "ncmecNumber")
    has_ncmec       = ncmec_number is not None

    # --- Identity ---
    subj_id         = case.get("subjectIdentification", {})
    first_name      = subj_id.get("firstName")
    middle_name     = subj_id.get("middleName")
    last_name       = subj_id.get("lastName")
    nicknames       = subj_id.get("nicknames")
    age_missing_min = subj_id.get("computedMissingMinAge")
    age_missing_max = subj_id.get("computedMissingMaxAge")
    current_age_min = subj_id.get("currentMinAge")
    current_age_max = subj_id.get("currentMaxAge")

    # --- Description ---
    subj_desc       = case.get("subjectDescription", {})
    sex             = get(subj_desc, "sex", "name")
    ethnicity       = get(subj_desc, "primaryEthnicity", "name")
    ethnicity_other = subj_desc.get("ethnicitiesOther")
    height_from     = subj_desc.get("heightFrom")   # inches
    height_to       = subj_desc.get("heightTo")
    weight_from     = subj_desc.get("weightFrom")   # lbs
    weight_to       = subj_desc.get("weightTo")
    tribal_affil    = get(subj_desc, "tribalAffiliation", "name")
    tribe_names     = "; ".join(
        t["tribe"]["tribeName"]
        for t in subj_desc.get("tribeAssociations", [])
        if get(t, "tribe", "tribeName")
    ) or None

    # --- Physical description ---
    phys            = case.get("physicalDescription", {})
    hair_color      = get(phys, "hairColor", "name")
    left_eye        = get(phys, "leftEyeColor", "name")
    right_eye       = get(phys, "rightEyeColor", "name")
    hair_desc       = phys.get("headHairDescription")
    eye_desc        = phys.get("eyeDescription")
    facial_hair     = phys.get("facialHairDescription")
    body_hair       = phys.get("bodyHairDescription")

    # --- Physical features (scars, tattoos etc.) ---
    features        = case.get("physicalFeatureDescriptions", [])
    feature_types   = "; ".join(
        get(f, "physicalFeature", "name", default="")
        for f in features if get(f, "physicalFeature", "name")
    ) or None
    feature_descs   = "; ".join(
        f["description"] for f in features if f.get("description")
    ) or None
    no_known_features = get(
        case, "physicalFeaturesNoKnownInformationIndicator", "noKnownInformation"
    )

    # --- Clothing ---
    clothing_items  = case.get("clothingAndAccessoriesArticles", [])
    clothing        = "; ".join(
        c["description"] for c in clothing_items if c.get("description")
    ) or None

    # --- Location & sighting ---
    sighting        = case.get("sighting", {})
    date_missing    = sighting.get("date")
    address         = sighting.get("address", {})
    city            = address.get("city")
    state           = get(address, "state", "name")
    county          = get(address, "county", "name")
    zip_code        = address.get("zipCode")
    lat             = get(sighting, "publicGeolocation", "coordinates", "lat")
    lon             = get(sighting, "publicGeolocation", "coordinates", "lon")
    tribal_land     = get(sighting, "missingFromTribalLand", "name")
    tribal_residence = get(sighting, "primaryResidenceOnTribalLand", "name")

    # --- Circumstances & notes ---
    circumstances   = get(case, "circumstances", "circumstancesOfDisappearance")
    notes           = "; ".join(
        n["description"] for n in case.get("notes", []) if n.get("description")
    ) or None

    # --- Investigating agency ---
    agencies        = case.get("investigatingAgencies", [])
    agency_name     = get(case, "primaryInvestigatingAgency", "name")
    agency_state    = get(agencies[0], "state", "name") if agencies else None
    agency_county   = get(agencies[0], "countyDisplayName") if agencies else None
    date_reported   = agencies[0].get("dateReported") if agencies else None
    agency_case_num = agencies[0].get("caseNumber") if agencies else None

    # --- Vehicles ---
    vehicle_list    = [v for v in case.get("vehicles", []) if v.get("vehicleMake") or v.get("vehicleModel")]
    has_vehicle     = bool(vehicle_list)
    vehicle_make    = vehicle_list[0].get("vehicleMake") if vehicle_list else None
    vehicle_model   = vehicle_list[0].get("vehicleModel") if vehicle_list else None
    vehicle_year    = vehicle_list[0].get("vehicleYear") if vehicle_list else None
    vehicle_color   = vehicle_list[0].get("vehicleColor") if vehicle_list else None
    vehicle_state   = vehicle_list[0].get("tagState") if vehicle_list else None

    # --- States (our addition) ---
    search_states   = case.get("searchStates", [])
    is_multistate   = len(search_states) > 1
    search_states_str = "; ".join(search_states)

    # --- Derived ---
    year_missing    = int(date_missing[:4]) if date_missing else None
    year_reported   = int(date_reported[:4]) if date_reported else None

    days_missing = None
    if date_missing:
        try:
            missing_dt = date.fromisoformat(date_missing)
            days_missing = (TODAY - missing_dt).days
        except ValueError:
            pass

    reporting_delay = None
    if date_missing and date_reported:
        try:
            days_to_report = (
                date.fromisoformat(date_reported) - date.fromisoformat(date_missing)
            ).days
            reporting_delay = days_to_report
        except ValueError:
            pass

    # Average age at disappearance
    if age_missing_min is not None and age_missing_max is not None:
        age_missing_avg = (age_missing_min + age_missing_max) / 2
    else:
        age_missing_avg = None

    return {
        # Core
        "case_id":              case_id,
        "is_resolved":          is_resolved,
        "created_date":         created,
        "modified_date":        modified,
        "has_image":            has_image,
        "has_document":         has_document,
        "has_ncmec":            has_ncmec,
        "ncmec_number":         ncmec_number,
        # Identity
        "first_name":           first_name,
        "middle_name":          middle_name,
        "last_name":            last_name,
        "nicknames":            nicknames,
        "age_missing_min":      age_missing_min,
        "age_missing_max":      age_missing_max,
        "age_missing_avg":      age_missing_avg,
        "current_age_min":      current_age_min,
        "current_age_max":      current_age_max,
        # Description
        "sex":                  sex,
        "ethnicity":            ethnicity,
        "ethnicity_other":      ethnicity_other,
        "height_from_in":       height_from,
        "height_to_in":         height_to,
        "weight_from_lbs":      weight_from,
        "weight_to_lbs":        weight_to,
        "tribal_affiliation":   tribal_affil,
        "tribe_names":          tribe_names,
        # Physical
        "hair_color":           hair_color,
        "left_eye_color":       left_eye,
        "right_eye_color":      right_eye,
        "hair_description":     hair_desc,
        "eye_description":      eye_desc,
        "facial_hair":          facial_hair,
        "body_hair":            body_hair,
        "physical_features":    feature_types,
        "physical_feature_desc": feature_descs,
        "no_known_features":    no_known_features,
        "clothing":             clothing,
        # Location
        "date_missing":         date_missing,
        "year_missing":         year_missing,
        "days_missing":         days_missing,
        "city":                 city,
        "state":                state,
        "county":               county,
        "zip_code":             zip_code,
        "lat":                  lat,
        "lon":                  lon,
        "missing_from_tribal_land":      tribal_land,
        "primary_residence_tribal_land": tribal_residence,
        # Circumstances
        "circumstances":        circumstances,
        "notes":                notes,
        # Agency
        "investigating_agency": agency_name,
        "agency_state":         agency_state,
        "agency_county":        agency_county,
        "agency_case_number":   agency_case_num,
        "date_reported":        date_reported,
        "year_reported":        year_reported,
        "reporting_delay_days": reporting_delay,
        # Vehicle
        "has_vehicle":          has_vehicle,
        "vehicle_make":         vehicle_make,
        "vehicle_model":        vehicle_model,
        "vehicle_year":         vehicle_year,
        "vehicle_color":        vehicle_color,
        "vehicle_state":        vehicle_state,
        # States
        "search_states":        search_states_str,
        "is_multistate":        is_multistate,
    }


def main():
    print("Loading data...")
    with open(INPUT_FILE) as f:
        data = json.load(f)
    print(f"  {len(data)} cases loaded")

    print("Cleaning cases...")
    rows = [extract_case(case) for case in data]

    df = pd.DataFrame(rows)

    print("Output summary:")
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")
    print()

    # Coverage report
    print(f"{'Column':<35} {'Non-null':>10} {'Coverage':>10}")
    print("-" * 58)
    for col in df.columns:
        non_null = df[col].notna().sum()
        pct = non_null / len(df) * 100
        print(f"{col:<35} {non_null:>10} {pct:>9.1f}%")

    print(f"\nWriting to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")


main()
