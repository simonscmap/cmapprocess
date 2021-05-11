# This is the prepping/processing script for the Tang modified dataset. Dataset was updated by Rosie Gradoville and Kendra Turk_Kubo.

import pandas as pd
from cmapingest import data
from cmapingest import vault_structure as vs


names = [
    "source_data",
    "source_location_of_data",
    "source_comments_from_Tang_validation",
    "data_cross_check_complete",
    "UCYN_A_qPCR_assays_used",
    "methods_sampling_analysis",
    "time",
    "lat",
    "lon",
    "depth",
    "UCYN_A1_nifH_gene",
    "UCYN_A2_A3_A4_nifH_gene",
    "total_UCYN_A__nifH_gene",
    "UCYN_B_nifH_gene",
    "UCYN_C_nifH_gene",
    "trichodesmium_nifH_gene",
    "richelia_nifH_gene",
    "calothrix_nifH_gene",
    "richelia_associated_species",
    "calothrix_associated_species",
    "notes",
]
df = pd.read_excel("Tang_2019_CMAP_v5.xlsx", names=names, sheet_name="nifH_Gene")
rdf = df[
    [
        "time",
        "lat",
        "lon",
        "depth",
        "UCYN_A1_nifH_gene",
        "UCYN_A2_A3_A4_nifH_gene",
        "total_UCYN_A__nifH_gene",
        "UCYN_B_nifH_gene",
        "UCYN_C_nifH_gene",
        "trichodesmium_nifH_gene",
        "richelia_nifH_gene",
        "calothrix_nifH_gene",
        "richelia_associated_species",
        "calothrix_associated_species",
        "notes",
        "source_data",
        "source_location_of_data",
        "source_comments_from_Tang_validation",
        "data_cross_check_complete",
        "UCYN_A_qPCR_assays_used",
        "methods_sampling_analysis",
    ]
]
cdf = data.clean_data_df(rdf)
# cdf.to_excel(vs.combined + 'Tang_Modified_nifH.xlsx',sheet_name = 'data',index=False)
