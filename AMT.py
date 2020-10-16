"""This python script is an assorted collection of functions to process data from the AMT (Atlantic Meridional Transect) cruise program."""


"""AMT finishing plan:
write new datafiles with new name
manually merge new data and metadata
manually run MARKDOWN on all metadata...


"""

import pandas as pd
import numpy as np
import os
import sys
import glob

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data




data_base_path = vs.collected_data + 'AMT_cruise_data/'
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
    try:
        df['time'] = pd.to_datetime(df["time"],format='%d/%m/%Y %H:%M')
    except:
        df['time'] = pd.to_datetime(df["time"])

    df = data.clean_data_df(df)

    cruise_name = df["cruise"].unique()[0]
    cleaned_df, dum_df = remove_non_var_columns(df)
    cleaned_df = timelatlon_reorder(cleaned_df)

    dataset_meta, vars_meta = built_meta_DataFrame()

    """add to dataset_metadata"""
    dataset_meta["dataset_version"] = "Final"
    dataset_meta["dataset_make"] = "observation"
    dataset_meta["dataset_source"] = "Atlantic Meridional Transect (AMT)"
    dataset_meta["dataset_distributor"] = "British Oceanographic Data Centre (BODC)"

    dataset_meta["cruise_names"] = [cruise_name]
    dataset_meta["dataset_description"] = [dum_df.to_string()]
    """add to vars_metadata"""
    vars_meta["var_short_name"] = list(cleaned_df)
    if "Data Flags" in meta_html[0].iloc[0].to_string():
        # vars_meta["var_comment"][vars_meta["var_short_name"].str.contains("flag")] = meta_html[0].to_string(index=False)
                vars_meta["var_comment"][vars_meta["var_short_name"].str.contains("flag")] = flow_cyto_dataflag_markdown()
    for col in ['time','lat','lon']:
        vars_meta = vars_meta[~vars_meta["var_short_name"].str.contains(col)]
    return cleaned_df, dataset_meta, vars_meta

def flow_cyto_dataflag_markdown():
    markdown = pd.DataFrame({'Flag': ["NaN","<",">","A","B","C","D","E","G","H","I","K","L","M","N","O","P","Q","R","S","T","U","W","X"], 'Description': ["Unqualified", "Below detection limit", "In excess of quoted value", "Taxonomic flag for affinis (aff.)", "Beginning of CTD Down/Up Cast", "Taxonomic flag for confer (cf.)", "Thermometric depth", "End of CTD Down/Up Cast", "Non-taxonomic biological characteristic uncertainty", "Extrapolated value", "Taxonomic flag for single species (sp.)", "Improbable value - unknown quality control source", "Improbable value - originators quality control", "Improbable value - BODC quality control", "Null value", "Improbable value - user quality control", "Trace/calm", "Indeterminate", "Replacement value", "Estimated value", "Interpolated value", "Uncalibrated", "Control value", "Excessive difference"]}).to_markdown()
    return markdown


#################
#    AMT 01     #
#################

"""bathemetry"""
def process_amt_01_bathemetry():
    meta_html = pd.read_html(data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_bathemetry_meta.html")
    df = pd.read_csv(data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_bathemetry.txt",sep='\t',skiprows=15,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','velocity_east','velocity_east_flag','velocity_north','velocity_north_flag',	'distance_traveled','distance_traveled_flag',	'heading','heading_flag',	'bathymetric_depth','bathymetric_depth_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_bathemetry.xlsx',cleaned_df,dataset_meta, vars_meta)
                                                                                               
"""meteorology"""
def process_amt_01_meterology():
    meta_html = pd.read_html(data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_meteorology_meta.html")
    df = pd.read_csv(data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_meteorology.txt",sep='\t',skiprows=17,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','air_pressure','air_pressure_flag','air_temperature','air_temperature_flag','solar_radiation','solar_radiation_flag','SurfVPAR','SurfVPAR_flag','relative_wind_dir','relative_wind_dir_flag','relative_wind_speed','relative_wind_speed_flag','wind_direction_from','wind_direction_from_flag','wind_speed','wind_speed_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_meterology.xlsx',cleaned_df,dataset_meta, vars_meta)

"""TSG"""
def process_amt_01_tsg():
    meta_html = pd.read_html(data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_tsg_meta.html")
    df = pd.read_csv(data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_tsg.txt",sep='\t',skiprows=13,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','chl_a','chl_a_flag','fluorescence','fluorescence_flag','salinity','salinity_flag','temperature','temperature_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_TSG.xlsx',cleaned_df,dataset_meta, vars_meta)

"""CTD"""
def process_amt_01_CTD():
    meta_html = pd.read_html(data_base_path + "AMT01/CTD/CTD/1056481.html")
    txt_files = glob.glob(data_base_path + 'AMT01/CTD/CTD/*.txt')
    new = [pd.read_csv(f,sep='\t',skiprows=13,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','sensor_pressure','sensor_pressure_flag','potential_temperature','potential_temperature_flag','practical_salinity','practical_salinity_flag','sigma_theta','sigma_theta_flag','uncalibrated_CTD_temperature','uncalibrated_CTD_temperature_flag']) for f in txt_files]
    df_concat = pd.concat(new).fillna(method='ffill')
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df_concat,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_CTD.xlsx',cleaned_df,dataset_meta, vars_meta)

"""Nutrient"""
def process_amt_01_Nutrient():
    meta_html = pd.read_html(data_base_path + "AMT01//Nutrient/Nutrient/1093184.html")
    txt_files = glob.glob(data_base_path + 'AMT01//Nutrient/Nutrient/*.txt')
    new = [pd.read_csv(f,sep='\t',skiprows=15,index_col=False,names=['cruise','station','type','time','lon','lat','local_cdi_ID','EDMO_code','bottle_depth','depth_below_surf','depth_below_surf_flag','C22_flag','C22_flag_quality','nitrate','nitrate_flag','nitrate_nitrite','nitrate_nitrite_flag','phosphate','phosphate_flag','sample_reference','sample_reference_flag','silicate','silicate_flag']) for f in txt_files]
    df_concat = pd.concat(new).fillna(method='ffill')
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df_concat,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR19950921_AMT01_Nutrient.xlsx',cleaned_df,dataset_meta, vars_meta)



#################
#    AMT 02     #
#################


#################
#    AMT 18     #
#################
def process_amt_18_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT18/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT18/flow_cytometry/AMT18_JR20081003_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'date_edit_tblJR20081003_AMT18_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)


def process_amt_19_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT19/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT19/flow_cytometry/AMT19_JC039_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJC039_AMT19_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)


def process_amt_20_flow_cyto_unstained():
    meta_html = pd.read_html(data_base_path + "AMT20/flow_cytometry/unstained/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT20/flow_cytometry/unstained/AMT20_JC053_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",                       'cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJC053_AMT20_unstained_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_20_flow_cyto_stained():
    meta_html = pd.read_html(data_base_path + "AMT20/flow_cytometry/stained/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT20/flow_cytometry/stained/AMT20_AFC.csv",skiprows=1,names=["CTD","lat","lon","bottle_number","depth","bacterioplankton","high_nucleic_acid_containing_bacteria","low_nucleic_acid_containing_bacteria","prochlorococcus_cyanobacteria","aplastidic_protists","plastidic_protists"])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJC053_AMT20_stained_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_21_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT21/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT21/flow_cytometry/AMT21_D371_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",                       'cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblD371_AMT21_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_22_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT22/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT22/flow_cytometry/AMT22_JC079_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJC079_AMT22_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_23_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT23/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT23/flow_cytometry/AMT23_JR20131005_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","synechococcus_abundance_P700A90Z_Tarran","synechococcus_abundance_P700A90Z_Tarran_flag","picoeukaryotic_abundance_PYEUA00A_Tarran","picoeukaryotic_abundance_PYEUA00A_Tarran_flag","prymnesiophyceae_abundance_P490A00Z","prymnesiophyceae_abundance_P490A00Z_flag","nanoeukaryotic_abundance_X726A86B","nanoeukaryotic_abundance_X726A86B_flag","prochlorococcus_abundance_P701A90Z_Tarran","prochlorococcus_abundance_P701A90Z_Tarran_flag","cryptophyceae_abundance_J79A0596","cryptophyceae_abundance_J79A0596_flag"])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR20131005_AMT23_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_24_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT24/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT24/flow_cytometry/AMT24_JR20140922_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference","cryptophyceae_abundance_J79A0596","cryptophyceae_abundance_J79A0596_flag","prymnesiophyceae_abundance_P490A00Z","prymnesiophyceae_abundance_P490A00Z_flag","synechococcus_abundance_P700A90Z_Tarran","synechococcus_abundance_P700A90Z_Tarran_flag","prochlorococcus_abundance_P701A90Z_Tarran","prochlorococcus_abundance_P701A90Z_Tarran_flag","picoeukaryotic_abundance_PYEUA00A_Tarran","picoeukaryotic_abundance_PYEUA00A_Tarran_flag","nanoeukaryotic_abundance_X726A86BX726A86B","nanoeukaryotic_abundance_X726A86BX726A86B_flag"])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR20140922_AMT24_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_25_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT25/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT25/flow_cytometry/AMT25_JR15001_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR15001_AMT25_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_26_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT26/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT26/flow_cytometry/AMT26_JR16001_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'bacteria_abundance_C804B6A6','bacteria_abundance_C804B6A6_flag','cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','bacteria_abundance_P18318A9','bacteria_abundance_P18318A9_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR16001_AMT26_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_27_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT27/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT27/flow_cytometry/AMT27_DY084_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'bacteria_abundance_C804B6A6','bacteria_abundance_C804B6A6_flag','cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','bacteria_abundance_P18318A9','bacteria_abundance_P18318A9_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblDY084_AMT27_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_28_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT28/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT28/flow_cytometry/AMT28_JR18001_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'bacteria_abundance_C804B6A6','bacteria_abundance_C804B6A6_flag','cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','bacteria_abundance_P18318A9','bacteria_abundance_P18318A9_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblJR18001_AMT28_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)

def process_amt_29_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT29/flow_cytometry/FlagData.html")
    df = pd.read_csv(data_base_path + "AMT29/flow_cytometry/AMT29_DY110_AFC_Dataset.csv",skiprows=1,names=["cruise","bodc_station","original_station","ODV_type","gear","lat","lon","w_depth","time","site","depth","bottle_pressure","BODC_bottle_code","bottle_flag","rosette_position","firing_sequence","bottle_reference",'bacteria_abundance_C804B6A6','bacteria_abundance_C804B6A6_flag','cryptophyceae_abundance_J79A0596','cryptophyceae_abundance_J79A0596_flag','bacteria_abundance_P18318A9','bacteria_abundance_P18318A9_flag','prymnesiophyceae_abundance_P490A00Z','prymnesiophyceae_abundance_P490A00Z_flag','synechococcus_abundance_P700A90Z_Tarran','synechococcus_abundance_P700A90Z_Tarran_flag','prochlorococcus_abundance_P701A90Z_Tarran','prochlorococcus_abundance_P701A90Z_Tarran_flag','picoeukaryotic_abundance_PYEUA00A_Tarran','picoeukaryotic_abundance_PYEUA00A_Tarran_flag','nanoeukaryotic_abundance_X726A86B','nanoeukaryotic_abundance_X726A86B_flag'])
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df,meta_html)
    combine_df_to_excel(vs.staging + 'combined/' + 'tblDY110_AMT29_flow_cytometry.xlsx',cleaned_df,dataset_meta, vars_meta)


# process_amt_01_bathemetry()
# process_amt_01_tsg()
# process_amt_01_meterology()
# process_amt_01_CTD()
# process_amt_01_Nutrient()

# process_amt_18_flow_cyto()
# process_amt_19_flow_cyto()
# process_amt_20_flow_cyto_unstained()
# process_amt_21_flow_cyto()
# process_amt_22_flow_cyto()
# process_amt_23_flow_cyto()
# process_amt_24_flow_cyto()
# process_amt_25_flow_cyto()
# process_amt_26_flow_cyto()
# process_amt_27_flow_cyto()
# process_amt_28_flow_cyto()
# process_amt_29_flow_cyto()