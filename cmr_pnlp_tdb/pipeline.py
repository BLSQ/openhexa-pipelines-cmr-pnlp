import json
import typing
import tempfile
import io
import os

import pandas as pd
import geopandas as gpd
import papermill as pm

from datetime import date, datetime
from shapely.geometry import shape
from itertools import product

from epiweeks import Week, Year

from openhexa.sdk import current_run, parameter, pipeline, workspace

from openhexa.toolbox.dhis2 import DHIS2

@pipeline("cmr-pnlp-tdb", name="CMR PNLP TdB de Routine")
@parameter(
    "get_year",
    name="Year",
    help="Year for which to extract and process data",
    type=int,
    default=2023,
    required=False,
)
@parameter(
    "get_download_routine",
    name="Download new routine data?",
    help="Download new data or reprocess existing extracts",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "get_download_mape",
    name="Download new MAPE data?",
    help="Download new data or reprocess existing extracts",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "get_download_pop",
    name="Download new population data?",
    help="Download new data or reprocess existing extracts",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "get_run_notebooks",
    name="Process data",
    help="Whether or not to push the processed results to the dashboard DB",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "get_upload",
    name="Upload?",
    help="Whether or not to push the processed results to the dashboard DB",
    type=bool,
    default=True,
    required=False,
)
def cmr_pnlp_tdb(
    get_year, 
    get_download_routine, 
    get_download_mape, 
    get_download_pop, 
    get_run_notebooks,
    get_upload,
    *args, 
    **kwargs):
    """
    """
    
    # setup variables
    PROJ_ROOT = f'{workspace.files_path}/Analysis/Routine TDB/'
    DATA_DIR = f'{PROJ_ROOT}data/'
    RAW_DATA_DIR = f'{DATA_DIR}raw/'

    INPUT_NB = f'{PROJ_ROOT}LAUNCHER-auto.ipynb'
    OUTPUT_NB_DIR = f'{PROJ_ROOT}papermill-outputs/'

    if get_download_routine:
        # extract data from DHIS
        routine_download_complete = extract_dhis_data(
            RAW_DATA_DIR, 
            get_year, 
            'routine'
        )

    if get_download_mape:
        # extract data from DHIS
        mape_download_complete = extract_dhis_data(
            RAW_DATA_DIR, 
            get_year, 
            'mape'
        )

    if get_download_pop:
        pop_download_complete = extract_dhis_data(
            RAW_DATA_DIR, 
            get_year, 
            'population'
        )

    # run processing code in notebook
    params = {
        'ANNEE': get_year, 
        'UPLOAD': get_upload
    }

    if get_run_notebooks:
        if get_download_mape:
            ppml = run_papermill_script(
                INPUT_NB, 
                OUTPUT_NB_DIR, 
                params, 
                mape_download_complete
        )
        else:
            ppml = run_papermill_script(INPUT_NB, OUTPUT_NB_DIR, params)


@cmr_pnlp_tdb.task
def extract_dhis_data(output_dir, year, mode, *args, **kwargs):

    extract_period = get_dhis_period(year, mode)

    current_run.log_info(f"Extracting analytics data for {mode} ({extract_period})")
    
    dhis2_download_analytics(
        output_dir, 
        extract_period, 
        mode
    )

    return 0  

@cmr_pnlp_tdb.task
def run_papermill_script(in_nb, out_nb_dir, parameters, *args, **kwargs):
    current_run.log_info(f"Running code in {in_nb}")

    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")
    out_nb = f"{out_nb_dir}{os.path.basename(in_nb)}_OUTPUT_{execution_timestamp}.ipynb"

    pm.execute_notebook(in_nb, out_nb, parameters)
    return    

#### helper functions ####
def dhis2_download_analytics(output_dir, extract_period, mode, *args, **kwargs):
    """
    Extracts DHIS2 data for dashboard.

    Mode: routine (malaria data elements) or all cause mortality (indicator)
    extract_period: list of periods to extract in DHIS period format (ex 202204)

    """
    
    # establish DHIS2 connection
    connection = workspace.dhis2_connection('cmr-snis')
    dhis2 = DHIS2(connection=connection, cache_dir = None) # f'{workspace.files_path}/temp/')

    ous = pd.DataFrame(dhis2.meta.organisation_units())
    fosa_list = ous.loc[ous.level == 5].id.to_list()
    aire_list = ous.loc[ous.level == 4].id.to_list()

    # TEST
    # fosa_list = fosa_list[:3] 
    # aire_list = aire_list[:3] 

    # data elements for extract ex: ["aZwnLALknnj", "D3h3Qvl0332"]
    DATA_DIR = f'{workspace.files_path}/Analysis/Routine TDB/data/' # Todo: make global
    metadata_file_path = f'{DATA_DIR}CMR-PNLP-DE-Extracts - DE_mapping.csv'
    
    monitored_des = pd.read_csv(metadata_file_path)

    # define data dimension 
    routine = False

    if mode == 'routine':
        routine = True
        monitored_des = monitored_des.loc[monitored_des.Freq == 'Monthly']
        output_directory_name = 'routine'

    elif mode == 'mape':
        monitored_des = monitored_des.loc[monitored_des.Freq == 'Weekly']
        output_directory_name = 'mape'

    else:
        # population
        monitored_des = monitored_des.loc[monitored_des.Freq == 'Yearly']
        output_directory_name = 'population'

    monitored_des = monitored_des['DE ID'].unique().tolist()

    # run extraction requests for analytics data
    dhis2.analytics.MAX_DX = 10
    dhis2.analytics.MAX_OU = 10

    if routine:
        raw_data = dhis2.analytics.get(
                data_elements = monitored_des,
                periods = extract_period,
                org_units = fosa_list
            )
    else:
        raw_data = dhis2.analytics.get(
            data_elements = monitored_des,
            periods = extract_period,
            org_units = aire_list
        )

    df = pd.DataFrame(raw_data)

    # extract metadata
    current_run.log_info("Extracting + merging instance metadata")
    df = dhis2.meta.add_org_unit_name_column(dataframe=df)
    df = dhis2.meta.add_org_unit_parent_columns(dataframe=df)

    df = dhis2.meta.add_dx_name_column(dataframe=df)

    # add COCs to routine data elements
    df = dhis2.meta.add_coc_name_column(dataframe=df)


    # define output path and save file
    current_run.log_info("Creating analytics.csv")



    year = int(str(extract_period[0])[:4])
    output_dir = f'{output_dir}/{output_directory_name}/{year}'
    os.makedirs(output_dir, exist_ok=True)

    out_path = f'{output_dir}/analytics.csv'
    df.to_csv(out_path)

    return out_path

def get_dhis_period(year, mode):
    """ 
    Returns a list of lists of DHIS2 months (YYYYMM -- 202003) based on the year
    specified to the function. For the current year, all months up to N-1 are 
    included in the list. For previous years, the list contains all months.
    """

    current_date = date.today()

    period_start_month = 1
    period_end_month = 12

    if mode == 'routine' :
    
        # current year : up to last month
        # previous years: all months 
        if year == current_date.year:
            period_end_month = current_date.month - 1

            
        month_list = dhis_month_period_range(year, period_start_month, period_end_month)

        return month_list

    elif mode == 'mape':
        # current year : up to last week
        if year == current_date.year:
            current_epi_week = Week.fromdate(current_date).week
            
            week_list = [f'{year}W{x}' for x in range(1, current_epi_week)]
        
        # previous years: all weeks    
        else:
            week_list = [f'{year}W{x.week}' for x in Year(year).iterweeks()]
            
        return week_list


    else:
        return [year]

    
    

def dhis_month_period_range(year, start, end):
    r = [
        f'{year}{str(x).zfill(2)}' for x in 
        range(start, end + 1)
    ]

    return r



if __name__ == "__main__":
    cmr_pnlp_tdb()
