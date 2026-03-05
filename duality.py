import os
# from rbi_constants import *
from status_code import *
# from NABARD_FULL_CODE import *
# from nse_code import *
import csv
import time
from RBI_code_7_loop import *
from obligations_new_code import *

#DUALITY MODULE:
#Handles extraction of dual obligations from regulatory documents
#Uses obligations_new_code for core processing logic

def dump_error_info(error_path, content_list):
    error_csv_path = os.path.join(error_path, 'error.csv')
    file_exists = os.path.isfile(error_csv_path)

    with open(error_csv_path, mode='a', newline='') as error_file:
        error_writer = csv.writer(error_file)
        # Write header if the file did not exist
        if not file_exists:
            error_writer.writerow(['File Name', 'Status Code', 'Error Message'])
        error_writer.writerow(content_list)
        
def duality_extraction(input_path, base_output):
    print("Inside duality extraction")
    result = {}
    pdf_files = []
    error_count = 0  # Initialize error count
    excel_file_path = None
    error_file = []
    try:
        # Walk through the input path
        for dirpath, _, filenames in os.walk(input_path):
            for filename in filenames:
                if filename.lower().endswith('.pdf'):
                    full_path = os.path.join(dirpath, filename)
                    filename_without_extension = os.path.splitext(filename)[0]
                    output_path = base_output + str(filename_without_extension)
                    os.makedirs(output_path, exist_ok=True)
                    print("FULL PATH::",full_path)
                    print("OUTPUT PATH FROM DUALITY:::",output_path)
                    # set_output_path(output_path) 
                    # ret_val,main_df2 = main_reg(full_path, output_path)
                    ret_val= main_rbi(full_path, output_path)
                    print("RET VAL:::",ret_val)
                    print(ret_val['result'])
                    # a = os.path.join(output_path, 'reg_text.xlsx')
                    # main_df2.to_excel(a,index=False)
                    #import pdb; pdb.set_trace()
                    if ret_val.get('res_code') == STATUS_200:
                        print(f" Call obligation module")
                        excel_file_path = ret_val.get('result')  # [Chapter 1]
                        json_file_path = os.path.join(output_path, 'tab1.json')
                        print("PRINTING EXCEL FILE PATH================================================>")
                        print(excel_file_path)
                        print("PRINTING JSON FILE PATH==================================>")
                        print(json_file_path)

                        # Check if the json file exists
                        if os.path.exists(json_file_path):
                            result = rbi__ngo_api_2(excel_file_path, json_file_path)
                            result['Excel_path']=excel_file_path
                            print("Result after rbi__ngo_api_2",result)
                        else:
                            result = rbi__ngo_api_2(excel_file_path, None) 
                            print("Result after rbi__ngo_api_2 from else",result)
                        
                    else:
                        print(f" Dump the error {ret_val.get('status_str')}")
                        content_list = [full_path, ret_val.get('res_code'), ret_val.get('status_str')]
                        dump_error_info(output_path, content_list)
                        error_file.append(filename_without_extension)
                        error_count += 1  # Increment error count
                
        result.update({'res_code': STATUS_200, 'status_str': STR_200, 'result': pdf_files, 'error_count': error_count, 'error_file': error_file})

        return result,excel_file_path
    except Exception as es:
        result.update({'res_code': STATUS_500, 'status_str': str(es), 'result': None})
        return result, excel_file_path




#input_path = r"C:\Users\BJ574SU\Downloads\CLARA\obligation code\Input Files"
#input_path = r"Input Files/"

#base_output = r"C:\Users\BJ574SU\Downloads\CLARA\obligation code\Output Files"
#base_output = r"Output Files/"




# # List all files in the directory
# try:
#     files = os.listdir(input_path)
#     print("Files in directory:")
#     for file in files:
#         print(file)
# except Exception as e:
#     print(f"Error listing files: {e}")

start = time.time()
# #ret_val = duality_extraction(input_path,base_output)
# ret_val = duality_extraction(input_path, base_output)
# end = time.time()
# total_time = f"Runtime of the program is {end - start:0.2f} Sec"
# print(total_time)
# print(f">>>> Ret Val {ret_val} ")
