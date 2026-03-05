import os
from rbi_constants import *
from status_code import *
#from compiled1_new import *
from obligations_new_code import *
import csv
import time

def dump_error_info(error_path, content_list):
    error_csv_path = os.path.join(error_path, 'error.csv')
    file_exists = os.path.isfile(error_csv_path)

    with open(error_csv_path, mode='a', newline='') as error_file:
        error_writer = csv.writer(error_file)
        # Write header if the file did not exist
        if not file_exists:
            error_writer.writerow(['File Name', 'Status Code', 'Error Message'])
        error_writer.writerow(content_list)



def duality_extraction1(input_path):
    result = {}
    pdf_files = []
    error_count = 0  # Initialize error count
    try:


        # Walk through the input path
        #import pdb; pdb.set_trace()
        for dirpath, dirnames, filenames in os.walk(input_path):
            for filename in filenames:
                if filename.lower().endswith('.xlsx'):
                    excel_file_path = os.path.join(dirpath, filename)
                    print("EXCEL FILE PATH :::", excel_file_path)
                    
                    json_file_path = None
                    for json_filename in filenames:
                        if json_filename.lower().endswith('.json'):
                            json_file_path = os.path.join(dirpath, json_filename)
                            print("JSON FILE PATH :::", json_file_path)
                            break  
                    if json_file_path:
                        print("CALLING RBI NGO 2 WITH EXCEL AND JSON")
                        result = rbi__ngo_api_2(excel_file_path, json_file_path)
                    else:
                        print("CALLING RBI NGO 2 WITH EXCEL ONLY")
                        result = rbi__ngo_api_2(excel_file_path, None)
                # else:
                #     print(f" Dump the error {ret_val.get('status_str')}")
                #     content_list = [filename, ret_val.get('res_code'), ret_val.get('status_str')]
                #     dump_error_info(filename, content_list)
                #     error_count += 1  # Increment error count
        
        result.update({'res_code': STATUS_200, 'status_str': STR_200, 'result': pdf_files, 'error_count': error_count})

        return result
    except Exception as es:
        result.update({'res_code': STATUS_500, 'status_str': str(es), 'result': None})
        return result


#input_path = 'C:/Users/JV238ZP/OneDrive - EY/Documents/RBI/DELIVERABLES1/April_Deliverables/aaa/'
# input_path = r"C:/Users/QQ417YB/Compliance/Final_Code_Path/Output Files"
#base_output = 'C:/Users/JV238ZP/OneDrive - EY/Documents/NABARD/09dec/'


# List all files in the directory
# try:
#     files = os.listdir(input_path)
#     print("Files in directory:")
#     for file in files:
#         print(file)
# except Exception as e:
#     print(f"Error listing files: {e}")


# ret_val = duality_extraction1(input_path)