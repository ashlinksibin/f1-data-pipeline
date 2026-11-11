# F1 Data Pipeline — Ingest, Transform, Gold Layer
#
# Pulls Formula 1 historical data (1950-present) from the Ergast API (via the
# Jolpica mirror), lands it in a medallion-architecture Azure Data Lake
# (raw -> ingested -> presentation), and builds the star-schema Gold tables
# consumed by the Power BI report.
#
# SECURITY NOTE: this script reads the storage SAS token from an environment
# variable (F1_SAS_TOKEN). Never hardcode credentials in source control.
# Generate a SAS token from Azure Portal -> Storage Account -> Shared access
# signature, then set it locally with:
#   export F1_SAS_TOKEN="sv=...&sig=..."   (macOS/Linux)
#   $env:F1_SAS_TOKEN="sv=...&sig=..."     (PowerShell)

import io
import os
import time

import pandas as pd
import requests

STORAGE_ACCOUNT = "f1dataengstorage01"
BASE_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
ERGAST_BASE = "https://api.jolpi.ca/ergast/f1"

SAS_TOKEN = os.environ.get("F1_SAS_TOKEN")
if not SAS_TOKEN:
    raise RuntimeError(
        "Set the F1_SAS_TOKEN environment variable before running this script."
    )


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def read_parquet_from_storage(container: str, path: str) -> pd.DataFrame:
    """Read a parquet file from Blob storage via SAS token over plain HTTPS."""
    url = f"{BASE_URL}/{container}/{path}?{SAS_TOKEN}"
    resp = requests.get(url)
    resp.raise_for_status()
    return pd.read_parquet(io.BytesIO(resp.content))


def write_parquet_to_storage(df: pd.DataFrame, container: str, path: str) -> None:
    """Write a DataFrame to Blob storage as parquet via SAS token."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    url = f"{BASE_URL}/{container}/{path}?{SAS_TOKEN}"
    headers = {"x-ms-blob-type": "BlockBlob"}
    resp = requests.put(url, data=buffer.getvalue(), headers=headers)
    resp.raise_for_status()
    print(f"Uploaded {path} -> {container} ({resp.status_code})")


# ---------------------------------------------------------------------------
# Stage 1: Ingest — pull each Ergast endpoint, flatten, land as parquet
# ---------------------------------------------------------------------------

def ingest_simple_endpoint(endpoint_name: str, record_key: str, limit: int = 100) -> pd.DataFrame:
    """
    Fetch a simple (non-nested-results) Ergast endpoint with pagination.

    endpoint_name: e.g. 'races', 'drivers', 'constructors', 'circuits'
    record_key:    the key holding the list inside the response's *Table
                    object, e.g. 'Races', 'Drivers', 'Constructors', 'Circuits'
    """
    all_records = []
    offset = 0

    while True:
        url = f"{ERGAST_BASE}/{endpoint_name}.json?limit={limit}&offset={offset}"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        table_key = next(k for k in data["MRData"].keys() if k.endswith("Table"))
        batch = data["MRData"][table_key][record_key]
        if not batch:
            break

        all_records.extend(batch)
        offset += limit
        print(f"{endpoint_name}: fetched {len(all_records)} so far...")

    df = pd.json_normalize(all_records)
    df.columns = df.columns.str.replace(".", "_", regex=False)

    write_parquet_to_storage(df, "ingested-data", f"{endpoint_name}/{endpoint_name}.parquet")
    return df


def ingest_results(limit: int = 100, max_retries: int = 3) -> pd.DataFrame:
    """
    Fetch race results season-by-season.

    Results are nested two levels deep (Races[].Results[]), so they need a
    different flatten strategy than the simple endpoints. Fetching per-season
    (rather than one long offset-paginated loop) keeps a single flaky request
    from losing the whole dataset, and each season is retried independently
    on failure -- the Ergast/Jolpica mirror rate-limits fairly aggressively
    (HTTP 429) once you're pulling 50+ seasons back to back.
    """
    all_results = []
    failed_seasons = []

    for season in range(1950, 2027):
        offset = 0
        season_ok = True

        while True:
            url = f"{ERGAST_BASE}/{season}/results.json?limit={limit}&offset={offset}"

            data = None
            for attempt in range(max_retries):
                resp = requests.get(url, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    break
                print(f"Season {season}: status {resp.status_code}, retrying...")
                time.sleep(3)

            if data is None:
                season_ok = False
                break

            races_batch = data["MRData"]["RaceTable"]["Races"]
            if not races_batch:
                break

            for race in races_batch:
                for result in race.get("Results", []):
                    flat = pd.json_normalize(result).iloc[0].to_dict()
                    flat["season"] = race["season"]
                    flat["round"] = race["round"]
                    flat["raceName"] = race["raceName"]
                    all_results.append(flat)

            offset += limit
            time.sleep(0.3)  # be polite to the API between pages

        if not season_ok:
            failed_seasons.append(season)
        print(f"Season {season}: {'ok' if season_ok else 'FAILED'} "
              f"-- total rows so far: {len(all_results)}")
        time.sleep(0.5)  # be polite between seasons

    if failed_seasons:
        print(f"Seasons that failed and need a retry pass: {failed_seasons}")

    results_df = pd.DataFrame(all_results)
    results_df.columns = results_df.columns.str.replace(".", "_", regex=False)
    results_df = results_df.drop_duplicates(
        subset=["season", "round", "Driver_driverId", "number"]
    )

    write_parquet_to_storage(results_df, "ingested-data", "results/results.parquet")
    return results_df


# ---------------------------------------------------------------------------
# Stage 2: Transform — build the star-schema Gold layer for Power BI
# ---------------------------------------------------------------------------

def build_gold_layer() -> None:
    """Read the Ingested layer, build fact/dim tables, write to Presentation."""

    races = read_parquet_from_storage("ingested-data", "races/races.parquet")
    drivers = read_parquet_from_storage("ingested-data", "drivers/drivers.parquet")
    constructors = read_parquet_from_storage("ingested-data", "constructors/constructors.parquet")
    results = read_parquet_from_storage("ingested-data", "results/results.parquet")

    # --- fact_race_results ---
    fact_race_results = results.copy()

    numeric_cols = ["grid", "laps", "points", "position", "Time_millis"]
    for col in numeric_cols:
        fact_race_results[col] = pd.to_numeric(fact_race_results[col], errors="coerce")
    fact_race_results["season"] = fact_race_results["season"].astype(int)
    fact_race_results["round"] = fact_race_results["round"].astype(int)

    fact_race_results = fact_race_results.rename(columns={
        "Driver_driverId": "driverId",
        "Constructor_constructorId": "constructorId",
        "Time_millis": "raceTimeMillis",
        "Time_time": "raceTime",
    })

    # A handful of 1950s-60s races used relief drivers (one driver, two cars
    # in the same race), which breaks the naive (season, round, driverId)
    # grain. entry_seq disambiguates those without dropping the rows.
    fact_race_results["entry_seq"] = (
        fact_race_results.groupby(["season", "round", "driverId"]).cumcount() + 1
    )
    fact_race_results["race_key"] = (
        fact_race_results["season"].astype(str) + "-" + fact_race_results["round"].astype(str)
    )

    # --- dim_driver ---
    dim_driver = drivers.copy()
    dim_driver["dateOfBirth"] = pd.to_datetime(dim_driver["dateOfBirth"], errors="coerce")
    dim_driver = dim_driver[
        ["driverId", "givenName", "familyName", "dateOfBirth", "nationality",
         "permanentNumber", "code"]
    ]

    # --- dim_constructor ---
    dim_constructor = constructors[["constructorId", "name", "nationality"]].drop_duplicates()

    # --- dim_race (includes circuit + location, for the map visual) ---
    dim_race = races.copy()
    dim_race["season"] = dim_race["season"].astype(int)
    dim_race["round"] = dim_race["round"].astype(int)
    dim_race["date"] = pd.to_datetime(dim_race["date"], errors="coerce")
    dim_race["Circuit_Location_lat"] = pd.to_numeric(dim_race["Circuit_Location_lat"], errors="coerce")
    dim_race["Circuit_Location_long"] = pd.to_numeric(dim_race["Circuit_Location_long"], errors="coerce")
    dim_race["race_key"] = dim_race["season"].astype(str) + "-" + dim_race["round"].astype(str)
    dim_race = dim_race[[
        "season", "round", "race_key", "raceName", "date",
        "Circuit_circuitId", "Circuit_circuitName",
        "Circuit_Location_lat", "Circuit_Location_long",
        "Circuit_Location_locality", "Circuit_Location_country",
    ]]

    # Sanity check: grain should now be unique
    dupes = fact_race_results.groupby(
        ["season", "round", "driverId", "entry_seq"]
    ).size()
    assert (dupes > 1).sum() == 0, "fact_race_results grain is not unique!"

    write_parquet_to_storage(fact_race_results, "presentation-data",
                              "fact_race_results/fact_race_results.parquet")
    write_parquet_to_storage(dim_driver, "presentation-data",
                              "dim_driver/dim_driver.parquet")
    write_parquet_to_storage(dim_constructor, "presentation-data",
                              "dim_constructor/dim_constructor.parquet")
    write_parquet_to_storage(dim_race, "presentation-data",
                              "dim_race/dim_race.parquet")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Stage 1: Ingest")
    ingest_simple_endpoint("races", "Races")
    ingest_simple_endpoint("drivers", "Drivers")
    ingest_simple_endpoint("constructors", "Constructors")
    ingest_simple_endpoint("circuits", "Circuits")
    ingest_results()

    print("\nStage 2: Transform -> Gold layer")
    build_gold_layer()

    print("\nDone. Refresh the Power BI report to pick up the new data.")
