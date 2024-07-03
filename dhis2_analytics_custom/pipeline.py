import os
from datetime import datetime

import polars as pl
from openhexa.sdk import current_run, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string


@pipeline("dhis2-analytics-custom", name="DHIS2 Analytics Custom")
def dhis2_analytics_custom():
    get()


@dhis2_analytics_custom.task
def get():
    # Parameters
    CONNECTION = "cmr-snis"
    DATA_ELEMENTS = [
        "bAZvcaraYLS",
        "BxQ8HnJEMzG",
        "c3kSWLkdopg",
        "c7dLo74Ww9W",
        "Cf16e6FmTzX",
        "drO822KakJ6",
        "fK1DoNkTy7B",
        "fMq0YfpJQ6r",
        "gXhkPufIFa4",
        "hgT3APR8J97",
        "hoqab2QuF3h",
        "iLLroHcTxAV",
        "IPcjv2LbErd",
        "JoxXUjbWBnQ",
        "jSHpZfm0moe",
        "jV7zlrz8kgy",
        "k3aYUxdQFtc",
        "n94w97WVd2I",
        "SfhZNZUlqo8",
        "WBMFCAI93Y5",
        "Yl98UdGjD1U",
        "YXrRpgiIyGr",
        "Zc93OrBbmrY",
        "ZWo7w69n7mm",
    ]
    START = period_from_string("202401")
    END = period_from_string(datetime.now().strftime("%Y%m"))
    ORG_UNIT_LEVELS = [5]
    OUTPUT_DIR = "Datasets/rma_fosa/2024/T1/surveillance/PNLP_Donnees_rma_fosa_01_05"

    # connect to DHIS2
    dhis = DHIS2(workspace.dhis2_connection(CONNECTION))
    current_run.log_info(f"Connected to {dhis.api.url}")

    # create output dir if it does not exist
    output_dir = os.path.join(workspace.files_path, OUTPUT_DIR)
    os.makedirs(output_dir, exist_ok=True)

    # max elements per request
    dhis.analytics.MAX_DX = 50
    dhis.analytics.MAX_ORG_UNITS = 50
    dhis.analytics.MAX_PERIODS = 1

    # get list of periods from start and end
    prange = START.get_range(END)
    periods = [str(pe) for pe in prange]

    # get data
    data_values = dhis.analytics.get(
        data_elements=DATA_ELEMENTS,
        periods=periods,
        org_unit_levels=ORG_UNIT_LEVELS,
    )
    current_run.log_info(f"Extracted {len(data_values)} data values")

    # transform dataframe for improved readability
    df = pl.DataFrame(data_values)
    df = dhis.meta.add_dx_name_column(df)
    df = dhis.meta.add_coc_name_column(df)
    df = dhis.meta.add_org_unit_name_column(df)
    df = dhis.meta.add_org_unit_parent_columns(df)

    # write data
    fp = os.path.join(output_dir, "analytics.csv")
    df.write_csv(fp)
    current_run.add_file_output(fp)

    return


if __name__ == "__main__":
    dhis2_analytics_custom()
