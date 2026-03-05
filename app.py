# Entry point for the obligation extraction application
# Initializes duality extraction process
from duality import *
from duality_obligation import *
Input_folder=r"C:/Users/Rehan/Downloads/obligation code/src/input/"
base_output=r"C:/Users/Rehan/Downloads/obligation code/src/output/"
print("calling duality extraction")

processed_df, excel_file_path = duality_extraction(Input_folder, base_output)