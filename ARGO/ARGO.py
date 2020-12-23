#### DEVNOTE Dec 2nd, 20 ####
# Two tables (BGC) and (CORE)
# How to ID BGC? (Header?) fname list?
# According to this: https://euroargodev.github.io/argoonlineschool/L21_ArgoDatabyFloat_Prof.html
#     Then _prof suffix contains *all* profile data. In the future, use wget? to get not all profiles

# Write two functions. 
#1. Get all WMO in tblARGOTABLE, so new ones can be added
#2. Loop through all WMO, get header to group by CORE or BGC
#
# Next steps: once have list(s) of core/bgc
# clean function
# insert into test tbl
# create two metadata files
# insert metadata
#... etc..

 
 #dev note Dec 11th. Apparently an "S"prefix to the proj are 'synthetic' ie. containing both core and BGC. 
 #An index of the synthetic should be available at ftp://ftp.ifremer.fr/ifremer/argo/argo_synthetic-profile_index.txt
 # I've emailed to request auth for the FTP.


 #devnote on dec 21st: Dump metadata
 #which cols are data
 #which cols are metadata
 #loop through floats
 #for col in float, extract unique cols
 


import xarray as xr
import pandas as pd
import pycmap
import glob
import os
from cmapingest import data
from cmapingest import common
from cmapingest import vault_structure as vs
from tqdm import tqdm
import numpy as np




argo_base_dir = vs.collected_data + "ARGO/"
argo_daac_list = ['aoml','bodc','coriolis','csio','csiro','incois','jma','kma','kordi','meds','nmdis']

bgc_metadata_columns = [
'CONFIG_MISSION_NUMBER', 
'DATA_CENTRE',
'DATA_TYPE',
'DATE_CREATION',
'DATE_UPDATE',
'DIRECTION',
'FIRMWARE_VERSION',
'FLOAT_SERIAL_NO',
'FORMAT_VERSION',
'HANDBOOK_VERSION',
'PARAMETER', 
'PARAMETER_DATA_MODE',
'PI_NAME',
'PLATFORM_NUMBER',
'PLATFORM_TYPE',
'PROJECT_NAME',
'REFERENCE_DATE_TIME',
'STATION_PARAMETERS',
'WMO_INST_TYPE']

bgc_data_columns = [
'BBP470',
'BBP470_ADJUSTED',
'BBP470_ADJUSTED_ERROR',
'BBP470_ADJUSTED_QC',
'BBP470_QC',
'BBP470_dPRES',
'BBP532',
'BBP532_ADJUSTED',
'BBP532_ADJUSTED_ERROR',
'BBP532_ADJUSTED_QC',
'BBP532_QC',
'BBP532_dPRES',
'BBP700',
'BBP700_2',
'BBP700_2_ADJUSTED',
'BBP700_2_ADJUSTED_ERROR',
'BBP700_2_ADJUSTED_QC',
'BBP700_2_QC',
'BBP700_2_dPRES',
'BBP700_ADJUSTED',
'BBP700_ADJUSTED_ERROR',
'BBP700_ADJUSTED_QC',
'BBP700_QC',
'BBP700_dPRES',
'CDOM',
'CDOM_ADJUSTED',
'CDOM_ADJUSTED_ERROR',
'CDOM_ADJUSTED_QC',
'CDOM_QC',
'CDOM_dPRES',
'CHLA',
'CHLA_ADJUSTED',
'CHLA_ADJUSTED_ERROR',
'CHLA_ADJUSTED_QC',
'CHLA_QC',
'CHLA_dPRES',
'CP660',
'CP660_ADJUSTED',
'CP660_ADJUSTED_ERROR',
'CP660_ADJUSTED_QC',
'CP660_QC',
'CP660_dPRES',
'CYCLE_NUMBER',
'DOWNWELLING_PAR',
'DOWNWELLING_PAR_ADJUSTED',
'DOWNWELLING_PAR_ADJUSTED_ERROR',
'DOWNWELLING_PAR_ADJUSTED_QC',
'DOWNWELLING_PAR_QC',
'DOWNWELLING_PAR_dPRES',
'DOWN_IRRADIANCE380',
'DOWN_IRRADIANCE380_ADJUSTED',
'DOWN_IRRADIANCE380_ADJUSTED_ERROR',
'DOWN_IRRADIANCE380_ADJUSTED_QC',
'DOWN_IRRADIANCE380_QC',
'DOWN_IRRADIANCE380_dPRES',
'DOWN_IRRADIANCE412',
'DOWN_IRRADIANCE412_ADJUSTED',
'DOWN_IRRADIANCE412_ADJUSTED_ERROR',
'DOWN_IRRADIANCE412_ADJUSTED_QC',
'DOWN_IRRADIANCE412_QC',
'DOWN_IRRADIANCE412_dPRES',
'DOWN_IRRADIANCE443',
'DOWN_IRRADIANCE443_ADJUSTED',
'DOWN_IRRADIANCE443_ADJUSTED_ERROR',
'DOWN_IRRADIANCE443_ADJUSTED_QC',
'DOWN_IRRADIANCE443_QC',
'DOWN_IRRADIANCE443_dPRES',
'DOWN_IRRADIANCE490',
'DOWN_IRRADIANCE490_ADJUSTED',
'DOWN_IRRADIANCE490_ADJUSTED_ERROR',
'DOWN_IRRADIANCE490_ADJUSTED_QC',
'DOWN_IRRADIANCE490_QC',
'DOWN_IRRADIANCE490_dPRES',
'DOWN_IRRADIANCE555',
'DOWN_IRRADIANCE555_ADJUSTED',
'DOWN_IRRADIANCE555_ADJUSTED_ERROR',
'DOWN_IRRADIANCE555_ADJUSTED_QC',
'DOWN_IRRADIANCE555_QC',
'DOWN_IRRADIANCE555_dPRES',
'DOXY',
'DOXY2',
'DOXY2_ADJUSTED',
'DOXY2_ADJUSTED_ERROR',
'DOXY2_ADJUSTED_QC',
'DOXY2_QC',
'DOXY2_dPRES',
'DOXY_ADJUSTED',
'DOXY_ADJUSTED_ERROR',
'DOXY_ADJUSTED_QC',
'DOXY_QC',
'DOXY_dPRES',
'JULD',
'JULD_LOCATION',
'JULD_QC',
'LATITUDE',
'LONGITUDE',
'NITRATE',
'NITRATE_ADJUSTED',
'NITRATE_ADJUSTED_ERROR',
'NITRATE_ADJUSTED_QC',
'NITRATE_QC',
'NITRATE_dPRES',
'PH_IN_SITU_TOTAL',
'PH_IN_SITU_TOTAL_ADJUSTED',
'PH_IN_SITU_TOTAL_ADJUSTED_ERROR',
'PH_IN_SITU_TOTAL_ADJUSTED_QC',
'PH_IN_SITU_TOTAL_QC',
'PH_IN_SITU_TOTAL_dPRES',
'POSITIONING_SYSTEM',
'POSITION_QC',
'PRES',
'PRES_ADJUSTED',
'PRES_ADJUSTED_ERROR',
'PRES_ADJUSTED_QC',
'PRES_QC',
'PROFILE_BBP470_QC',
'PROFILE_BBP532_QC',
'PROFILE_BBP700_2_QC',
'PROFILE_BBP700_QC',
'PROFILE_CDOM_QC',
'PROFILE_CHLA_QC',
'PROFILE_CP660_QC',
'PROFILE_DOWNWELLING_PAR_QC',
'PROFILE_DOWN_IRRADIANCE380_QC',
'PROFILE_DOWN_IRRADIANCE412_QC',
'PROFILE_DOWN_IRRADIANCE443_QC',
'PROFILE_DOWN_IRRADIANCE490_QC',
'PROFILE_DOWN_IRRADIANCE555_QC',
'PROFILE_DOXY2_QC',
'PROFILE_DOXY_QC',
'PROFILE_NITRATE_QC',
'PROFILE_PH_IN_SITU_TOTAL_QC',
'PROFILE_PRES_QC',
'PROFILE_PSAL_QC',
'PROFILE_TEMP_QC',
'PROFILE_TURBIDITY_QC',
'PROFILE_UP_RADIANCE412_QC',
'PROFILE_UP_RADIANCE443_QC',
'PROFILE_UP_RADIANCE490_QC',
'PROFILE_UP_RADIANCE555_QC',
'PSAL',
'PSAL_ADJUSTED',
'PSAL_ADJUSTED_ERROR',
'PSAL_ADJUSTED_QC',
'PSAL_QC',
'PSAL_dPRES',
'SCIENTIFIC_CALIB_COEFFICIENT',
'SCIENTIFIC_CALIB_COMMENT',
'SCIENTIFIC_CALIB_DATE',
'SCIENTIFIC_CALIB_EQUATION',
'TEMP',
'TEMP_ADJUSTED',
'TEMP_ADJUSTED_ERROR',
'TEMP_ADJUSTED_QC',
'TEMP_QC',
'TEMP_dPRES',
'TURBIDITY',
'TURBIDITY_ADJUSTED',
'TURBIDITY_ADJUSTED_ERROR',
'TURBIDITY_ADJUSTED_QC',
'TURBIDITY_QC',
'TURBIDITY_dPRES',
'UP_RADIANCE412',
'UP_RADIANCE412_ADJUSTED',
'UP_RADIANCE412_ADJUSTED_ERROR',
'UP_RADIANCE412_ADJUSTED_QC',
'UP_RADIANCE412_QC',
'UP_RADIANCE412_dPRES',
'UP_RADIANCE443',
'UP_RADIANCE443_ADJUSTED',
'UP_RADIANCE443_ADJUSTED_ERROR',
'UP_RADIANCE443_ADJUSTED_QC',
'UP_RADIANCE443_QC',
'UP_RADIANCE443_dPRES',
'UP_RADIANCE490',
'UP_RADIANCE490_ADJUSTED',
'UP_RADIANCE490_ADJUSTED_ERROR',
'UP_RADIANCE490_ADJUSTED_QC',
'UP_RADIANCE490_QC',
'UP_RADIANCE490_dPRES',
'UP_RADIANCE555',
'UP_RADIANCE555_ADJUSTED',
'UP_RADIANCE555_ADJUSTED_ERROR',
'UP_RADIANCE555_ADJUSTED_QC',
'UP_RADIANCE555_QC',
'UP_RADIANCE555_dPRES']


def get_argo_synthetic():
    """[summary]

    Returns:
        list: list of argo float ID's that are synthetic
    """
    df = pd.read_csv("argo_synthetic-profile_index.txt",sep=',',skiprows=8)
    #splits float name column into only float ID. aoml/1900722/profiles/SD1900722_001.nc -> daac/1900722/
    syn_file_names = df['file'].str.split('profiles',expand=True)[0].unique()
    return syn_file_names


def get_synthetic_headers(argo_synthetic_list):
    bgc_vars_list = []
    missed_syn_list = []
    for syn_float in tqdm(argo_synthetic_list):
        try:
            float_profile = xr.open_dataset(argo_base_dir + syn_float +  syn_float.split('/')[1] +"_Sprof.nc")#.to_dataframe().reset_index()
            float_vars = list(float_profile.keys())
            bgc_vars_list.extend(float_vars)
        except Exception as e:
            print(e)
            missed_syn_list.append(syn_float)
    bgc_vars_list = np.sort(list(set(bgc_vars_list)))
    return bgc_vars_list, missed_syn_list



def glob_floats_daac(daac):
    base_dir = argo_base_dir + daac + "/"
    float_list = os.listdir(base_dir)
    return float_list

def reorder_bgc_data(df):
    """Reordered a BGC dataframe to move ST coodinates first

    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame

    Returns:
        df (Pandas DataFrame): Reordered DataFrame
    """
    st_col_list = ['JULD','LATITUDE','LONGITUDE','depth','float_id','CYCLE_NUMBER']
    st_cols =df[st_col_list]
    non_st_cols = df.drop(st_col_list, axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)
    reorder_rename_df = reorder_df.rename(columns={'JULD': 'time', 'LATITUDE': 'lat', 'LONGITUDE': 'lon', 'CYCLE_NUMBER': 'cycle'})
    return reorder_rename_df


def add_climatology_cols(df):
    """      
     [year]
      ,[month]
      ,[week]
      ,[dayofyear]

    Args:
        df ([type]): [description]

    Returns:
        [type]: [description]
    """
    return df


def format_bgc_metadata(df,bgc_metadata_columns):
    """Split off metadata columns from argo dataframe, find unique values, add to dictionary

    Args:
        df (Pandas DataFrame): Argo float DataFrame
        bgc_metadata_columns (list): List of all BGC 'metadata' columns

    Returns:
        dictionary: Dictionary of kvp for all argo metadata
    """
    mdf = df[bgc_metadata_columns]
    bgc_metadata_dict = {}
    for col in list(mdf):
        bgc_metadata_dict[col] = list(pd.Series(mdf[col].unique()).astype(str).str.strip())
    return bgc_metadata_dict


def format_split_bgc(syn_float, bgc_template_df):
    """load netcdf into dataframe"""
    float_profile = xr.open_dataset(argo_base_dir + syn_float +  syn_float.split('/')[1] +"_Sprof.nc").to_dataframe().reset_index()
    """decode binary strings"""
    df = data.decode_df_columns(float_profile)
    """"Adds any missing columns to data dataframe"""
    float_df = bgc_template_df.append(df)
    """calls func that returns dictionary of metadata"""
    bgc_metadata_dict = format_bgc_metadata(df,bgc_metadata_columns)
    """trims down df to only data columns"""
    bgc_df = float_df[bgc_data_columns]
    """adds depth as column"""
    bgc_df["depth"] =  bgc_df['PRES_ADJUSTED']
    """adds float ID column from float_path_name"""
    bgc_df["float_id"] = syn_float.split('/')[1]
    """reorders bgc_df with ST index leading followed by float_id and cycle"""
    reorder_df = reorder_bgc_data(bgc_df)
    return reorder_df

# format_bgc_metadata(df,bgc_metadata_columns)
bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])
syn_float = get_argo_synthetic()[0]
bgc_df = format_split_bgc(syn_float, bgc_template_df)

# for val in list(float_profile):
#     print(float_profile[val].value_counts())




# bgc_list = []
# core_list = []
# for argofloat in float_list:
#     float_profile = xr.open_dataset(argo_base_dir + daac + "/" + argofloat +"/" +  argofloat +"_prof.nc").to_dataframe().reset_index()
    
#     break

def clean_argo_df(df,float_ID):
    """This function cleans and renames variables in the raw argo dataframe

    Args:
        df (Pandas DataFrame): Input raw argo dataframe

    Returns:
        df (Pandas DataFrame): Output cleaned argo dataframe
    """
    # df = df[['direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']]
    df["float_id"] = float_ID
    df = add_depth_columns(df,"pres")
    
    # 1.   trim down df columns
    # 1.5  add float_ID col, calculate depth from pressure 
    # 2.   rename columns to match
    # 3.   reorder columsn to match
    # 4.   insert any missing columns
    # df = df[['cycle_number','direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']]
    df.rename()
    return df


# def compile_profiles(daac,float_ID):
#     float_profiles = glob.glob(argo_base_dir + daac + "/" + float_ID + "/" + "profiles/*.nc")
#     master_df = pd.DataFrame(columns = ['N_CALIB','N_HISTORY','N_LEVELS','N_PARAM','N_PROF','DATA_TYPE','FORMAT_VERSION','HANDBOOK_VERSION','REFERENCE_DATE_TIME','DATE_CREATION','DATE_UPDATE','PLATFORM_NUMBER','PROJECT_NAME','PI_NAME','STATION_PARAMETERS','CYCLE_NUMBER','DIRECTION','DATA_CENTRE','DC_REFERENCE','DATA_STATE_INDICATOR','DATA_MODE','PLATFORM_TYPE','FLOAT_SERIAL_NO','FIRMWARE_VERSION','WMO_INST_TYPE','JULD','JULD_QC','JULD_LOCATION','LATITUDE','LONGITUDE','POSITION_QC','POSITIONING_SYSTEM','VERTICAL_SAMPLING_SCHEME','CONFIG_MISSION_NUMBER','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PRES','PSAL','TEMP','PRES_QC','PSAL_QC','TEMP_QC','PRES_ADJUSTED','PSAL_ADJUSTED','TEMP_ADJUSTED','PRES_ADJUSTED_QC','PSAL_ADJUSTED_QC','TEMP_ADJUSTED_QC','PRES_ADJUSTED_ERROR','PSAL_ADJUSTED_ERROR','TEMP_ADJUSTED_ERROR','PARAMETER','SCIENTIFIC_CALIB_EQUATION','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','HISTORY_INSTITUTION','HISTORY_STEP','HISTORY_SOFTWARE','HISTORY_SOFTWARE_RELEASE','HISTORY_REFERENCE','HISTORY_DATE','HISTORY_ACTION','HISTORY_PARAMETER','HISTORY_START_PRES','HISTORY_STOP_PRES','HISTORY_PREVIOUS_VALUE','HISTORY_QCTEST'])
#     for argo_float in float_profiles:
#         #clean df here
#         df = xr.open_dataset(argo_float).to_dataframe().reset_index()
#         master_df = master_df.append(df)
#     return master_df



# compile_float = '2902297'
# daac = 'incois'
# # compile_df = compile_profiles(daac,compile_float)

# df = xr.open_dataset(argo_base_dir + daac + "/" + compile_float +"/" +  compile_float +"_prof.nc").to_dataframe().reset_index()
# xdf = xr.open_dataset(argo_base_dir + daac + "/" + compile_float +"/" +  compile_float +"_Sprof.nc")




def get_unique_cols(argo_synthetic_list):
    counter=10
    ucol_df = pd.DataFrame(columns=['float_ID','ucols'])
    float_id_list = []
    ucols_list = []
    for syn_float in tqdm(argo_synthetic_list[0:10]):
        df = xr.open_dataset(argo_base_dir + syn_float +  syn_float.split('/')[1] +"_Sprof.nc").to_dataframe().reset_index()
        df = data.decode_df_columns(df)
        print(df.PI_NAME.unique())
    #     uniqucols = []
    #     for val in list(df):
    #         numunique =  len(df[val].unique())
    #         if numunique == 1:
    #             uniqucols.append(val)
    #     float_id_list.append(syn_float)
    #     ucols_list.append(uniqucols)
    #     counter += 1
    #     if counter == 10:
    #         break
    # ucol_df.append(float_id_list,ucols_list)
    return ucol_df
# ucol_df = get_unique_cols(argo_synthetic_list)

