# PURPOSE: To extract aircraft data from FAA's site
# Since the function to extract the data only varies with a few parameters
# Thought it would be beneficial to use the "factory pattern":
# https://github.com/dagster-io/dagster/discussions/11045

from dagster import (
    AssetsDefinition,
    Definitions,
    MetadataValue,
    Output,
    asset,
)
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from io import BytesIO
import pandas as pd
import requests
import zipfile


# The following are what will be passed to our asset builder to build the individual assets
# I could have read this data from a file or database, but decided to just keep it here for simplicity
data_sets = [
    # "group" is being used to refer to duckdb schema, along with defining the asset group name in dagit
    {
        "group": "aircraft",
        "name": "aircraft",
        "url": "https://av-info.faa.gov/data/ACRef/tab/aircraft.zip",
        "date": "Last_Change_Date",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\ACREF",
    },
    {
        "group": "aircraft",
        "name": "ata_codes",
        "url": "https://av-info.faa.gov/data/ACREF/tab/ata.zip",
        "date": "Last_Change_Date",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\ACREF",
    },
    {
        "group": "aircraft",
        "name": "compt",
        "url": "https://av-info.faa.gov/data/ACREF/tab/compt.zip",
        "date": "Last_Change_Date",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\ACREF",
    },
    {
        "group": "aircraft",
        "name": "engine",
        "url": "https://av-info.faa.gov/data/ACREF/tab/engine.zip",
        "date": "Last_Change_Date",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\ACREF",
    },
    {
        "group": "aircraft",
        "name": "prop",
        "url": "https://av-info.faa.gov/data/ACREF/tab/prop.zip",
        "date": "Last_Change_Date",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\ACREF",
    },
    {
        "group": "reference",
        "name": "aircraft_ref",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/aircraft.zip",
        "date": None,
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "acseries",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/acseries.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "airmanuf",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/airmanuf.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "airport",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/airport.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "country",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/country.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "distoffc",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/distoffc.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "makemodl",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/makemodl.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "simulatr",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/simulatr.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
    {
        "group": "reference",
        "name": "state",
        "url": "https://av-info.faa.gov/data/REFERENCE/tab/state.zip",
        "date": "LCHG_DATE",
        "site": "https://av-info.faa.gov/dd_sublevel.asp?Folder=\\REFERENCE",
    },
]


# This function serves as a "template" that the asset builder below will use
def extract_aircraft_data(date: str, url: str) -> pd.DataFrame:
    # Download the zip file
    response = requests.get(url, timeout=15)

    # Check if the download was successful
    if response.status_code == 200:
        # Extract the contents in memory
        zip_data = BytesIO(response.content)
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            # Assuming the zip file contains only one CSV file, get its path
            csv_path = zip_ref.namelist()[0]
            # Read the CSV file into a pandas DataFrame directly from the zip file
            with zip_ref.open(csv_path) as csv_file:
                if date:
                    df = pd.read_csv(csv_file, delimiter="\t", parse_dates=[date])
                else:
                    df = pd.read_csv(csv_file, delimiter="\t")

    return df


def build_asset(data_set) -> AssetsDefinition:
    @asset(
        name=data_set["name"],
        group_name=data_set["group"],
        compute_kind="duckdb",
        io_manager_key="faa_io_manager",
        key_prefix=data_set["group"],  # the duckdb schema to use
    )
    def _asset() -> Output[pd.DataFrame]:
        df = extract_aircraft_data(data_set["date"], data_set["url"])

        return Output(
            value=df,
            metadata={
                "website": MetadataValue.md(
                    f"This data was obtained from this [website]({data_set['site']})."
                ),
                "preview": MetadataValue.md(df.head().to_markdown(index=False)),
            },
        )

    return _asset

