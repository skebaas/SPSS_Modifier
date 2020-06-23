#! /usr/bin/python3

import pandas as pd
import os
import sys

#Get current working directory
cwd = os.getcwd()

class Visit():


    def __init__(self, visit_num, raw_data, input_file, output_name):

        self.visit_num = visit_num
        self.raw_data = raw_data
        self.input_file = input_file
        self.output_name = output_name

        ## IMPORTANT ##
        ## THE FOLLOWING LIST HOLDS VARIABLES THAT DO NOT HAVE A RAW DATA NAME ##
        self.special_cases = []
        self.visit_dict = {}
    
    
    
    def get_Visit_Data(self):
        col_dict = {}
        visit_csv = pd.read_csv(self.input_file, delimiter = ',')
        #select the first two columns from the VISIT2 csv and put into new dataframe
        df = visit_csv.iloc[:,[0,1]]
        #iterate through the rows and create the dictionary
        for row in df.iterrows():
            row_names_raw = row[1]['Raw data name ']
            row_names_master = row[1]['Masterfile name ']
            #print(col_dict[row[1]])
            #check to see if the raw_data_name column equals " no raw data"
            if row_names_raw.lower() == "no raw data".lower():
                self.special_cases.append(row[1]['Masterfile name '])
            col_dict[row_names_master] = row_names_raw
        self.visit_dict = col_dict

    def write_New_Data(self):

        temp_dict = {}
        input_csv = pd.read_csv(self.raw_data, delimiter = ',')

        self.get_Visit_Data()
        for key, value in self.visit_dict.items():
            try:
                if value.lower() == "no raw data".lower():
                    temp_dict[key] = "NaN"

                else:
                    temp_dict[key] = input_csv[value]

            except KeyError:
                    print(f"WARNING: Could not find data for {value}")

        df = pd.DataFrame.from_dict(temp_dict)
        df.to_csv(self.output_name)

    def check_Path(self):
        '''function to check to see if the path exists for the given output script'''
        if os.path.exists(self.output_name):
            proceed = input(f"{self.output_name} already exists. Would you like to replace it (y/N)? ").lower()
            while proceed not in ['y', 'n']:
                proceed = input(f"Invalid Input: (y/N)?")
            if proceed == 'n':
                return False
            else:
                return True
        else:
            return True


    def run(self):
        run_boolean = self.check_Path()
        if not run_boolean:
            return "Proceeding to next subject ..."
        print(f"Using {self.input_file} for visit {self.visit_num} data... \n")
        self.write_New_Data()
        print(f"Creating {self.output_name} in '{cwd}' ... ")

if __name__ == "__main__":
    args = sys.argv
    arg_visit_num = int(args[1])
    arg_raw_data = str(os.path.abspath(args[2]))
    arg_input_file = str(os.path.abspath(args[3]))
    arg_output_name = str(args[4])
    V = Visit(arg_visit_num, arg_raw_data, arg_input_file, arg_output_name)
    V.run()


#V1 = Visit(1, '/data/old_computer/clinical_data/Raw_data.csv', '/data/old_computer/clinical_data/V1.csv', 'VISIT1.csv')
#V2 = Visit(2, '/data/old_computer/clinical_data/Raw_data_new.csv', '/data/old_computer/clinical_data/V2.csv', 'VISIT2.csv')
#V1.run()
#V2.run()

    