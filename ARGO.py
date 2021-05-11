#### DEVNOTE Dec 2nd, 20 ####
# Two tables (BGC) and (CORE)
# How to ID BGC? (Header?) fname list?
# According to this: https://euroargodev.github.io/argoonlineschool/L21_ArgoDatabyFloat_Prof.html
#     Then _prof suffix contains *all* profile data. In the future, use wget? to get not all profiles
# Write two functions.
# 1. Get all WMO in ARGOTABLE, so new ones can be added
# 2. Loop through all WMO, get header to group by CORE or BGC


import xarray as xr
import pandas as pd
import pycmap
import glob
import os
from cmapingest import data
from cmapingest import common
from cmapingest import vault_structure as vs


"""
1. iterate though daac and glob all floats in daac
2. for each float, iterate through and create master df
2.5: cleaning:
     -correct cols
     -column ordering
     -binary decoding
     -pressure to depth calculation
     -climatology conversion
3. clean master df and export into /vault/argo/float_ID/

"""


argo_base_dir = vs.collected_data + "ARGO/"
argo_daac_list = [
    "aoml",
    "bodc",
    "coriolis",
    "csio",
    "csiro",
    "incois",
    "jma",
    "kma",
    "kordi",
    "meds",
    "nmdis",
]


def glob_floats_daac(daac):
    base_dir = argo_base_dir + daac + "/"
    float_list = os.listdir(base_dir)
    return float_list


for daac in argo_daac_list:
    float_list = glob_floats_daac(daac)


def pres_to_depth(df, presure_column):
    df["depth"] = (df[presure_column] * 10) / (1023.6 * 9.80665)
    return df


def clean_argo_df(df, float_ID):
    """This function cleans and renames variables in the raw argo dataframe

    Args:
        df (Pandas DataFrame): Input raw argo dataframe

    Returns:
        df (Pandas DataFrame): Output cleaned argo dataframe
    """
    df = df[
        [
            "direction",
            "data_centre",
            "data_mode",
            "juld",
            "juld_qc",
            "juld_location",
            "latitude",
            "longitude",
            "position_qc",
            "pres",
            "psal",
            "temp",
            "pres_qc",
            "psal_qc",
            "temp_qc",
            "pres_adjusted",
            "psal_adjusted",
            "temp_adjusted",
            "pres_adjusted_qc",
            "psal_adjusted_qc",
            "temp_adjusted_qc",
            "pres_adjusted_error",
            "psal_adjusted_error",
            "temp_adjusted_error",
        ]
    ]
    df["float_id"] = float_ID
    df = add_depth_columns(df, "pres")

    # 1.   trim down df columns
    # 1.5  add float_ID col, calculate depth from pressure
    # 2.   rename columns to match
    # 3.   reorder columsn to match
    # 4.   insert any missing columns
    df = df[
        [
            "cycle_number",
            "direction",
            "data_centre",
            "data_mode",
            "juld",
            "juld_qc",
            "juld_location",
            "latitude",
            "longitude",
            "position_qc",
            "pres",
            "psal",
            "temp",
            "pres_qc",
            "psal_qc",
            "temp_qc",
            "pres_adjusted",
            "psal_adjusted",
            "temp_adjusted",
            "pres_adjusted_qc",
            "psal_adjusted_qc",
            "temp_adjusted_qc",
            "pres_adjusted_error",
            "psal_adjusted_error",
            "temp_adjusted_error",
        ]
    ]

    df.rename()
    return df


def compile_profiles(daac, float_ID):
    float_profiles = glob.glob(
        argo_base_dir + daac + "/" + float_ID + "/" + "profiles/*.nc"
    )
    master_df = pd.DataFrame(
        columns=[
            "N_CALIB",
            "N_HISTORY",
            "N_LEVELS",
            "N_PARAM",
            "N_PROF",
            "DATA_TYPE",
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PLATFORM_NUMBER",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            "CYCLE_NUMBER",
            "DIRECTION",
            "DATA_CENTRE",
            "DC_REFERENCE",
            "DATA_STATE_INDICATOR",
            "DATA_MODE",
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "JULD",
            "JULD_QC",
            "JULD_LOCATION",
            "LATITUDE",
            "LONGITUDE",
            "POSITION_QC",
            "POSITIONING_SYSTEM",
            "VERTICAL_SAMPLING_SCHEME",
            "CONFIG_MISSION_NUMBER",
            "PROFILE_PRES_QC",
            "PROFILE_PSAL_QC",
            "PROFILE_TEMP_QC",
            "PRES",
            "PSAL",
            "TEMP",
            "PRES_QC",
            "PSAL_QC",
            "TEMP_QC",
            "PRES_ADJUSTED",
            "PSAL_ADJUSTED",
            "TEMP_ADJUSTED",
            "PRES_ADJUSTED_QC",
            "PSAL_ADJUSTED_QC",
            "TEMP_ADJUSTED_QC",
            "PRES_ADJUSTED_ERROR",
            "PSAL_ADJUSTED_ERROR",
            "TEMP_ADJUSTED_ERROR",
            "PARAMETER",
            "SCIENTIFIC_CALIB_EQUATION",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
            "HISTORY_INSTITUTION",
            "HISTORY_STEP",
            "HISTORY_SOFTWARE",
            "HISTORY_SOFTWARE_RELEASE",
            "HISTORY_REFERENCE",
            "HISTORY_DATE",
            "HISTORY_ACTION",
            "HISTORY_PARAMETER",
            "HISTORY_START_PRES",
            "HISTORY_STOP_PRES",
            "HISTORY_PREVIOUS_VALUE",
            "HISTORY_QCTEST",
        ]
    )
    for argo_float in float_profiles:
        # clean df here
        df = xr.open_dataset(argo_float).to_dataframe().reset_index()
        master_df = master_df.append(df)
    return master_df


def add_depth_columns(df, pres_column):
    mdf["depth"] = ((mdf[pres_column] * 0.098692326671601) * 101325.0) / (1020.0 * 9.81)
    return df


compile_float = "2901615"
daac = "nmdis"
compile_df = compile_profiles(daac, compile_float)

float_profile = (
    xr.open_dataset(
        argo_base_dir + daac + "/" + compile_float + "/" + compile_float + "_prof.nc"
    )
    .to_dataframe()
    .reset_index()
)


# mdf.columns= mdf.columns.str.lower()
# existing_cols = ['float_id','cycle','time','lat','lon','depth','position_qc','direction','data_mode','data_centre','argo_merge_cdnc','argo_merge_cdnc_qc','argo_merge_cdnc_adj','argo_merge_cdnc_adj_qc','argo_merge_cdnc_adj_err','argo_merge_pressure','argo_merge_pressure_qc','argo_merge_pressure_adj','argo_merge_pressure_adj_qc','argo_merge_pressure_adj_err','argo_merge_salinity','argo_merge_salinity_qc','argo_merge_salinity_adj','argo_merge_salinity_adj_qc','argo_merge_salinity_adj_err','argo_merge_temperature','argo_merge_temperature_qc','argo_merge_temperature_adj','argo_merge_temperature_adj_qc','argo_merge_temperature_adj_err','argo_merge_O2','argo_merge_O2_qc','argo_merge_O2_adj','argo_merge_O2_adj_qc','argo_merge_O2_adj_err','argo_merge_bbp','argo_merge_bbp_qc','argo_merge_bbp_adj','argo_merge_bbp_adj_qc','argo_merge_bbp_adj_err','argo_merge_bbp470','argo_merge_bbp470_qc','argo_merge_bbp470_adj','argo_merge_bbp470_adj_qc','argo_merge_bbp470_adj_err','argo_merge_bbp532','argo_merge_bbp532_qc','argo_merge_bbp532_adj','argo_merge_bbp532_adj_qc','argo_merge_bbp532_adj_err','argo_merge_bbp700','argo_merge_bbp700_qc','argo_merge_bbp700_adj','argo_merge_bbp700_adj_qc','argo_merge_bbp700_adj_err','argo_merge_turbidity','argo_merge_turbidity_qc','argo_merge_turbidity_adj','argo_merge_turbidity_adj_qc','argo_merge_turbidity_adj_err','argo_merge_cp','argo_merge_cp_qc','argo_merge_cp_adj','argo_merge_cp_adj_qc','argo_merge_cp_adj_err','argo_merge_cp660','argo_merge_cp660_qc','argo_merge_cp660_adj','argo_merge_cp660_adj_qc','argo_merge_cp660_adj_err','argo_merge_chl','argo_merge_chl_qc','argo_merge_chl_adj','argo_merge_chl_adj_qc','argo_merge_chl_adj_err','argo_merge_cdom','argo_merge_cdom_qc','argo_merge_cdom_adj','argo_merge_cdom_adj_qc','argo_merge_cdom_adj_err','argo_merge_NO3','argo_merge_NO3_qc','argo_merge_NO3_adj','argo_merge_NO3_adj_qc','argo_merge_NO3_adj_err','argo_merge_bisulfide','argo_merge_bisulfide_qc','argo_merge_bisulfide_adj','argo_merge_bisulfide_adj_qc','argo_merge_bisulfide_adj_err','argo_merge_ph','argo_merge_ph_qc','argo_merge_ph_adj','argo_merge_ph_adj_qc','argo_merge_ph_adj_err','argo_merge_down_irr','argo_merge_down_irr_qc','argo_merge_down_irr_adj','argo_merge_down_irr_adj_qc','argo_merge_down_irr_adj_err','argo_merge_down_irr380','argo_merge_down_irr380_qc','argo_merge_down_irr380_adj','argo_merge_down_irr380_adj_qc','argo_merge_down_irr380_adj_err','argo_merge_down_irr412','argo_merge_down_irr412_qc','argo_merge_down_irr412_adj','argo_merge_down_irr412_adj_qc','argo_merge_down_irr412_adj_err','argo_merge_down_irr443','argo_merge_down_irr443_qc','argo_merge_down_irr443_adj','argo_merge_down_irr443_adj_qc','argo_merge_down_irr443_adj_err','argo_merge_down_irr490','argo_merge_down_irr490_qc','argo_merge_down_irr490_adj','argo_merge_down_irr490_adj_qc','argo_merge_down_irr490_adj_err','argo_merge_down_irr555','argo_merge_down_irr555_qc','argo_merge_down_irr555_adj','argo_merge_down_irr555_adj_qc','argo_merge_down_irr555_adj_err','argo_merge_up_irr','argo_merge_up_irr_qc','argo_merge_up_irr_adj','argo_merge_up_irr_adj_qc','argo_merge_up_irr_adj_err','argo_merge_up_irr412','argo_merge_up_irr412_qc','argo_merge_up_irr412_adj','argo_merge_up_irr412_adj_qc','argo_merge_up_irr412_adj_err','argo_merge_up_irr443','argo_merge_up_irr443_qc','argo_merge_up_irr443_adj','argo_merge_up_irr443_adj_qc','argo_merge_up_irr443_adj_err','argo_merge_up_irr490','argo_merge_up_irr490_qc','argo_merge_up_irr490_adj','argo_merge_up_irr490_adj_qc','argo_merge_up_irr490_adj_err','argo_merge_up_irr555','argo_merge_up_irr555_qc','argo_merge_up_irr555_adj','argo_merge_up_irr555_adj_qc','argo_merge_up_irr555_adj_err','argo_merge_down_par','argo_merge_down_par_qc','argo_merge_down_par_adj','argo_merge_down_par_adj_qc','argo_merge_down_par_adj_err','year','month','week','dayofyear']
# retained_cols = ['direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']

# ['float_id',
#  'cycle',
#  'time',
#  'lat',
#  'lon',
#  'depth',
#  'position_qc',
#  'direction',
#  'data_mode',
#  'data_centre',
#  'argo_merge_cdnc',
#  'argo_merge_cdnc_qc',
#  'argo_merge_cdnc_adj',
#  'argo_merge_cdnc_adj_qc',
#  'argo_merge_cdnc_adj_err',
#  'argo_merge_pressure',
#  'argo_merge_pressure_qc',
#  'argo_merge_pressure_adj',
#  'argo_merge_pressure_adj_qc',
#  'argo_merge_pressure_adj_err',
#  'argo_merge_salinity',
#  'argo_merge_salinity_qc',
#  'argo_merge_salinity_adj',
#  'argo_merge_salinity_adj_qc',
#  'argo_merge_salinity_adj_err',
#  'argo_merge_temperature',
#  'argo_merge_temperature_qc',
#  'argo_merge_temperature_adj',
#  'argo_merge_temperature_adj_qc',
#  'argo_merge_temperature_adj_err',
#  'argo_merge_O2',
#  'argo_merge_O2_qc',
#  'argo_merge_O2_adj',
#  'argo_merge_O2_adj_qc',
#  'argo_merge_O2_adj_err',
#  'argo_merge_bbp',
#  'argo_merge_bbp_qc',
#  'argo_merge_bbp_adj',
#  'argo_merge_bbp_adj_qc',
#  'argo_merge_bbp_adj_err',
#  'argo_merge_bbp470',
#  'argo_merge_bbp470_qc',
#  'argo_merge_bbp470_adj',
#  'argo_merge_bbp470_adj_qc',
#  'argo_merge_bbp470_adj_err',
#  'argo_merge_bbp532',
#  'argo_merge_bbp532_qc',
#  'argo_merge_bbp532_adj',
#  'argo_merge_bbp532_adj_qc',
#  'argo_merge_bbp532_adj_err',
#  'argo_merge_bbp700',
#  'argo_merge_bbp700_qc',
#  'argo_merge_bbp700_adj',
#  'argo_merge_bbp700_adj_qc',
#  'argo_merge_bbp700_adj_err',
#  'argo_merge_turbidity',
#  'argo_merge_turbidity_qc',
#  'argo_merge_turbidity_adj',
#  'argo_merge_turbidity_adj_qc',
#  'argo_merge_turbidity_adj_err',
#  'argo_merge_cp',
#  'argo_merge_cp_qc',
#  'argo_merge_cp_adj',
#  'argo_merge_cp_adj_qc',
#  'argo_merge_cp_adj_err',
#  'argo_merge_cp660',
#  'argo_merge_cp660_qc',
#  'argo_merge_cp660_adj',
#  'argo_merge_cp660_adj_qc',
#  'argo_merge_cp660_adj_err',
#  'argo_merge_chl',
#  'argo_merge_chl_qc',
#  'argo_merge_chl_adj',
#  'argo_merge_chl_adj_qc',
#  'argo_merge_chl_adj_err',
#  'argo_merge_cdom',
#  'argo_merge_cdom_qc',
#  'argo_merge_cdom_adj',
#  'argo_merge_cdom_adj_qc',
#  'argo_merge_cdom_adj_err',
#  'argo_merge_NO3',
#  'argo_merge_NO3_qc',
#  'argo_merge_NO3_adj',
#  'argo_merge_NO3_adj_qc',
#  'argo_merge_NO3_adj_err',
#  'argo_merge_bisulfide',
#  'argo_merge_bisulfide_qc',
#  'argo_merge_bisulfide_adj',
#  'argo_merge_bisulfide_adj_qc',
#  'argo_merge_bisulfide_adj_err',
#  'argo_merge_ph',
#  'argo_merge_ph_qc',
#  'argo_merge_ph_adj',
#  'argo_merge_ph_adj_qc',
#  'argo_merge_ph_adj_err',
#  'argo_merge_down_irr',
#  'argo_merge_down_irr_qc',
#  'argo_merge_down_irr_adj',
#  'argo_merge_down_irr_adj_qc',
#  'argo_merge_down_irr_adj_err',
#  'argo_merge_down_irr380',
#  'argo_merge_down_irr380_qc',
#  'argo_merge_down_irr380_adj',
#  'argo_merge_down_irr380_adj_qc',
#  'argo_merge_down_irr380_adj_err',
#  'argo_merge_down_irr412',
#  'argo_merge_down_irr412_qc',
#  'argo_merge_down_irr412_adj',
#  'argo_merge_down_irr412_adj_qc',
#  'argo_merge_down_irr412_adj_err',
#  'argo_merge_down_irr443',
#  'argo_merge_down_irr443_qc',
#  'argo_merge_down_irr443_adj',
#  'argo_merge_down_irr443_adj_qc',
#  'argo_merge_down_irr443_adj_err',
#  'argo_merge_down_irr490',
#  'argo_merge_down_irr490_qc',
#  'argo_merge_down_irr490_adj',
#  'argo_merge_down_irr490_adj_qc',
#  'argo_merge_down_irr490_adj_err',
#  'argo_merge_down_irr555',
#  'argo_merge_down_irr555_qc',
#  'argo_merge_down_irr555_adj',
#  'argo_merge_down_irr555_adj_qc',
#  'argo_merge_down_irr555_adj_err',
#  'argo_merge_up_irr',
#  'argo_merge_up_irr_qc',
#  'argo_merge_up_irr_adj',
#  'argo_merge_up_irr_adj_qc',
#  'argo_merge_up_irr_adj_err',
#  'argo_merge_up_irr412',
#  'argo_merge_up_irr412_qc',
#  'argo_merge_up_irr412_adj',
#  'argo_merge_up_irr412_adj_qc',
#  'argo_merge_up_irr412_adj_err',
#  'argo_merge_up_irr443',
#  'argo_merge_up_irr443_qc',
#  'argo_merge_up_irr443_adj',
#  'argo_merge_up_irr443_adj_qc',
#  'argo_merge_up_irr443_adj_err',
#  'argo_merge_up_irr490',
#  'argo_merge_up_irr490_qc',
#  'argo_merge_up_irr490_adj',
#  'argo_merge_up_irr490_adj_qc',
#  'argo_merge_up_irr490_adj_err',
#  'argo_merge_up_irr555',
#  'argo_merge_up_irr555_qc',
#  'argo_merge_up_irr555_adj',
#  'argo_merge_up_irr555_adj_qc',
#  'argo_merge_up_irr555_adj_err',
#  'argo_merge_down_par',
#  'argo_merge_down_par_qc',
#  'argo_merge_down_par_adj',
#  'argo_merge_down_par_adj_qc',
#  'argo_merge_down_par_adj_err',
#  'year',
#  'month',
#  'week',
#  'dayofyear']


# ['n_calib',
#  'n_history',
#  'n_levels',
#  'n_param',
#  'n_prof',
#  'data_type',
#  'format_version',
#  'handbook_version',
#  'reference_date_time',
#  'date_creation',
#  'date_update',
#  'platform_number',
#  'project_name',
#  'pi_name',
#  'station_parameters',
#  'cycle_number',
#  'direction',
#  'data_centre',
#  'dc_reference',
#  'data_state_indicator',
#  'data_mode',
#  'platform_type',
#  'float_serial_no',
#  'firmware_version',
#  'wmo_inst_type',
#  'juld',
#  'juld_qc',
#  'juld_location',
#  'latitude',
#  'longitude',
#  'position_qc',
#  'positioning_system',
#  'vertical_sampling_scheme',
#  'config_mission_number',
#  'profile_pres_qc',
#  'profile_psal_qc',
#  'profile_temp_qc',
#  'pres',
#  'psal',
#  'temp',
#  'pres_qc',
#  'psal_qc',
#  'temp_qc',
#  'pres_adjusted',
#  'psal_adjusted',
#  'temp_adjusted',
#  'pres_adjusted_qc',
#  'psal_adjusted_qc',
#  'temp_adjusted_qc',
#  'pres_adjusted_error',
#  'psal_adjusted_error',
#  'temp_adjusted_error',
#  'parameter',
#  'scientific_calib_equation',
#  'scientific_calib_coefficient',
#  'scientific_calib_comment',
#  'scientific_calib_date',
#  'history_institution',
#  'history_step',
#  'history_software',
#  'history_software_release',
#  'history_reference',
#  'history_date',
#  'history_action',
#  'history_parameter',
#  'history_start_pres',
#  'history_stop_pres',
#  'history_previous_value',
#  'history_qctest',
#  'inst_reference',
#  'calibration_date']
