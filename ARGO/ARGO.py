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


import xarray as xr
import pandas as pd
import pycmap
import glob
import os
from cmapingest import data
from cmapingest import common
from cmapingest import vault_structure as vs
from tqdm import tqdm


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
argo_daac_list = ['aoml','bodc','coriolis','csio','csiro','incois','jma','kma','kordi','meds','nmdis']



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
        except:
            missed_syn_list.append(syn_float)
    return bgc_vars_list, missed_syn_list



def glob_floats_daac(daac):
    base_dir = argo_base_dir + daac + "/"
    float_list = os.listdir(base_dir)
    return float_list

def reorder_bgc(df):
    """Reordered a BGC dataframe to move ST coodinates first

    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame

    Returns:
        df (Pandas DataFrame): Reordered DataFrame
    """
    st_col_list = ['FLOAT','CYCLE_NUMBER','JULD','LATITUDE','LONGITUDE','depth']
    st_cols =df[st_col_list]
    non_st_cols = df.drop(st_col_list, axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)

    return reorder_df


def insert_bgc_in_template(syn_float, bgc_template_df):
    float_profile = xr.open_dataset(argo_base_dir + syn_float +  syn_float.split('/')[1] +"_Sprof.nc").to_dataframe().reset_index()
    float_profile["depth"] =  float_profile['PRES_ADJUSTED']
    float_profile["FLOAT"] = syn_float.split('/')[1]
    float_df = bgc_template_df.append(float_profile)#.dropna(axis=1,how='all')
    reorder_df = reorder_bgc(float_df)
    decode_df = data.decode_df_columns(reorder_df)
    return decode_df

bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])
syn_float = get_argo_synthetic()[0]
bgc_df = insert_bgc_in_template(syn_float, bgc_template_df)

# for daac in argo_daac_list:
#     float_list = glob_floats_daac(daac)

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
    df = df[['direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']]
    df["float_id"] = float_ID
    df = add_depth_columns(df,"pres")
    
    # 1.   trim down df columns
    # 1.5  add float_ID col, calculate depth from pressure 
    # 2.   rename columns to match
    # 3.   reorder columsn to match
    # 4.   insert any missing columns
    df = df[['cycle_number','direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']]
    df.rename()
    return df


def compile_profiles(daac,float_ID):
    float_profiles = glob.glob(argo_base_dir + daac + "/" + float_ID + "/" + "profiles/*.nc")
    master_df = pd.DataFrame(columns = ['N_CALIB','N_HISTORY','N_LEVELS','N_PARAM','N_PROF','DATA_TYPE','FORMAT_VERSION','HANDBOOK_VERSION','REFERENCE_DATE_TIME','DATE_CREATION','DATE_UPDATE','PLATFORM_NUMBER','PROJECT_NAME','PI_NAME','STATION_PARAMETERS','CYCLE_NUMBER','DIRECTION','DATA_CENTRE','DC_REFERENCE','DATA_STATE_INDICATOR','DATA_MODE','PLATFORM_TYPE','FLOAT_SERIAL_NO','FIRMWARE_VERSION','WMO_INST_TYPE','JULD','JULD_QC','JULD_LOCATION','LATITUDE','LONGITUDE','POSITION_QC','POSITIONING_SYSTEM','VERTICAL_SAMPLING_SCHEME','CONFIG_MISSION_NUMBER','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PRES','PSAL','TEMP','PRES_QC','PSAL_QC','TEMP_QC','PRES_ADJUSTED','PSAL_ADJUSTED','TEMP_ADJUSTED','PRES_ADJUSTED_QC','PSAL_ADJUSTED_QC','TEMP_ADJUSTED_QC','PRES_ADJUSTED_ERROR','PSAL_ADJUSTED_ERROR','TEMP_ADJUSTED_ERROR','PARAMETER','SCIENTIFIC_CALIB_EQUATION','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','HISTORY_INSTITUTION','HISTORY_STEP','HISTORY_SOFTWARE','HISTORY_SOFTWARE_RELEASE','HISTORY_REFERENCE','HISTORY_DATE','HISTORY_ACTION','HISTORY_PARAMETER','HISTORY_START_PRES','HISTORY_STOP_PRES','HISTORY_PREVIOUS_VALUE','HISTORY_QCTEST'])
    for argo_float in float_profiles:
        #clean df here
        df = xr.open_dataset(argo_float).to_dataframe().reset_index()
        master_df = master_df.append(df)
    return master_df



# compile_float = '2902297'
# daac = 'incois'
# compile_df = compile_profiles(daac,compile_float)

# float_profile = xr.open_dataset(argo_base_dir + daac + "/" + compile_float +"/" +  compile_float +"_prof.nc").to_dataframe().reset_index()
# prof = xr.open_dataset(argo_base_dir + daac + "/" + compile_float +"/" +  compile_float +"_Sprof.nc")


# float_vars = list(float_profile.keys())


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
