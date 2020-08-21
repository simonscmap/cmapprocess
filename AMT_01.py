import pandas as pd
import numpy as np
import os
import sys
import glob

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data




#For each dataset input tablename, datafile_name, metadata_name"""
""" data should go to staging/ so it can be run though the validator..."""


data_base_path = vs.collected_data + 'AMT_cruise_data/AMT01'
# data_base_path = "/home/nrhagen/Desktop/AMT_cruise_data/AMT01/"


""" generalized funcs"""
def timelatlon_reorder(df):
    st_cols = df[['time','lat','lon']]
    non_st_cols = df.drop(['time','lat','lon'], axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)
    return reorder_df

def built_meta_DataFrame():
    dataset_meta = pd.DataFrame(columns=['dataset_short_name', 'dataset_long_name', 'dataset_version', 'dataset_release_date', 'dataset_make', 'dataset_source', 'dataset_distributor', 'dataset_acknowledgement', 'dataset_history', 'dataset_description', 'dataset_references', 'climatology', 'cruise_names'])
    vars_meta = pd.DataFrame(columns=['var_short_name', 'var_long_name', 'var_sensor', 'var_unit', 'var_spatial_res', 'var_temporal_res', 'var_discipline', 'visualize', 'var_keywords', 'var_comment'])
    return dataset_meta, vars_meta

def combine_df_to_excel(filename,df,dataset_metadata, vars_metadata):
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='data',index=False)
    dataset_metadata.to_excel(writer, sheet_name='dataset_meta_data',index=False)
    vars_metadata.to_excel(writer, sheet_name='vars_meta_data',index=False)
    writer.save()

def remove_non_var_columns(df):
    name_list = []
    data_list = []
    for col in list(df):
        if int(len(df[col].unique())) == 1:
            print(col,'dropped')
            name_list.append(col)
            data_list.append(df[col].unique()[0])
            df = df.drop(col, 1)
    dum_df = pd.DataFrame(data=[data_list],columns=name_list)
    return df, dum_df

def shared_AMT_data_processing(df,meta_html):
    df = data.clean_data_df(df)
    cruise_name = df["cruise"].unique()[0]
    cleaned_df, dum_df = remove_non_var_columns(df)
    cleaned_df = timelatlon_reorder(cleaned_df)

    dataset_meta, vars_meta = built_meta_DataFrame()

    """add to dataset_metadata"""
    dataset_meta["cruise_names"] = [cruise_name]
    dataset_meta["dataset_description"] = [dum_df.to_string()]
    """add to vars_metadata"""
    vars_meta["var_short_name"] = list(cleaned_df)
    vars_meta["var_comment"][vars_meta["var_short_name"].str.contains("flag")] = meta_html[-1].to_string(index=False)
    for col in ['time','lat','lon']:
        vars_meta = vars_meta[~vars_meta["var_short_name"].str.contains(col)]
    return cleaned_df, dataset_meta, vars_meta



"""bathemetry"""
def process_amt_01_bathemetry():
    meta_html = pd.read_html(data_base_path + "Bathymetry_Meteorology_TSG/amt_01_bathemetry_meta.html")
    df = pd.read_csv(data_base_path + "Bathymetry_Meteorology_TSG/amt_01_bathemetry.txt",sep='\t',skiprows=15,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','velocity_east','velocity_east_flag','velocity_north','velocity_north_flag',	'distance_traveled','distance_traveled_flag',	'heading','heading_flag',	'bathymetric_depth','bathymetric_depth_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_bathemetry.xlsx',cleaned_df,dataset_meta, vars_meta)
                                                                                               
"""meteorology"""
def process_amt_01_meterology():
    meta_html = pd.read_html(data_base_path + "Bathymetry_Meteorology_TSG/amt_01_meteorology_meta.html")
    df = pd.read_csv(data_base_path + "Bathymetry_Meteorology_TSG/amt_01_meteorology.txt",sep='\t',skiprows=17,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','air_pressure','air_pressure_flag','air_temperature','air_temperature_flag','solar_radiation','solar_radiation_flag','SurfVPAR','SurfVPAR_flag','relative_wind_dir','relative_wind_dir_flag','relative_wind_speed','relative_wind_speed_flag','wind_direction_from','wind_direction_from_flag','wind_speed','wind_speed_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_meterology.xlsx',cleaned_df,dataset_meta, vars_meta)

"""TSG"""
def process_amt_01_tsg():
    meta_html = pd.read_html(data_base_path + "Bathymetry_Meteorology_TSG/amt_01_tsg_meta.html")
    df = pd.read_csv(data_base_path + "Bathymetry_Meteorology_TSG/amt_01_tsg.txt",sep='\t',skiprows=13,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','chl_a','chl_a_flag','fluorescence','fluorescence_flag','salinity','salinity_flag','temperature','temperature_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_TSG.xlsx',cleaned_df,dataset_meta, vars_meta)


"""CTD"""
def process_amt_01_CTD():
    meta_html = pd.read_html(data_base_path + "/CTD/CTD/1056481.html")
    txt_files = glob.glob(data_base_path + '/CTD/CTD/*.txt')
    new = [pd.read_csv(f,sep='\t',skiprows=13,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','sensor_pressure','sensor_pressure_flag','potential_temperature','potential_temperature_flag','practical_salinity','practical_salinity_flag','sigma_theta','sigma_theta_flag','uncalibrated_CTD_temperature','uncalibrated_CTD_temperature_flag']) for f in txt_files]
    df_concat = pd.concat(new)
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df_concat,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_CTD.xlsx',cleaned_df,dataset_meta, vars_meta)


"""Nutrient"""
def process_amt_01_Nutrient():
    meta_html = pd.read_html(data_base_path + "/Nutrient/Nutrient/1093184.html")
    txt_files = glob.glob(data_base_path + '/Nutrient/Nutrient/*.txt')
    new = [pd.read_csv(f,sep='\t',skiprows=15,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','C22_flag','C22_flag_quality','nitrate','nitrate_flag','nitrate_nitrite','nitrate_nitrite_flag','phosphate','phosphate_flag','sample_reference','sample_reference_flag','silicate','silicate_flag']) for f in txt_files]
    df_concat = pd.concat(new)
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df_concat,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_Nutrient.xlsx',cleaned_df,dataset_meta, vars_meta)





# process_amt_01_bathemetry()
# process_amt_01_tsg()
# process_amt_01_meterology()
# process_amt_01_CTD()
process_amt_01_Nutrient()

