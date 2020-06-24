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
            #check to see if the raw_data_name column equals " no raw data"
            if row_names_raw.lower() == "no raw data".lower():
                self.special_cases.append(row[1]['Masterfile name '])
            col_dict[row_names_master] = row_names_raw
        self.visit_dict = col_dict

    def write_New_Data(self):

        output_dict = {}
        input_csv = pd.read_csv(self.raw_data, delimiter = ',')
        total_rows = len(input_csv)
        special_cases = self.special_cases
        

        # Run method that pulls the data from the input csv
        self.get_Visit_Data()

        #uncomment to see what Values are being included in the special_cases list
        #print(special_cases)


        # List that holds all VOCAB_Q* (Used for calculating Sum for VOCAB_raw_score)
        VOCAB_QUESTIONS = [key for key, val in self.visit_dict.items() if "VOCAB_Q" in key]
        MATRIX_ITEMS = [key for key, val in self.visit_dict.items() if "MATRIX_ITEM_" in key]

        
        ###Now the complicated part###
        '''
        The following section is looking through the new and old names:

        new_value: (String) What the raw data variable is being renamed to
        old_value: (String) Variable representing the value from the raw data 
        '''
        for new_value, old_value in self.visit_dict.items():
            try:

                if new_value in special_cases or new_value in VOCAB_QUESTIONS or new_value in MATRIX_ITEMS:
                    print(f"RUNNING: Performing neccessary calculations for {new_value}")
                    output_dict[new_value] = {}
                    for index, row in input_csv.iterrows():

                        if new_value in ["VOCAB_Q1_corrected", "VOCAB_Q2_corrected", "VOCAB_Q3_corrected", "VOCAB_Q4_corrected"]:
                            output_dict[new_value][index] = 1
                        elif new_value in ["VOCAB_Q5_corrected", "VOCAB_Q6_corrected", "VOCAB_Q7_corrected", "VOCAB_Q8_corrected"]:
                            output_dict[new_value][index] = 2
                        elif new_value in VOCAB_QUESTIONS:
                            try:
                                vocab_value = int(row[old_value]) -1
                            except ValueError:
                                vocab_value = row[old_value]
                            output_dict[new_value][index] = vocab_value
                        elif new_value == "VOCAB_raw_score":
                            subj_total = 0
                            for question in VOCAB_QUESTIONS:
                                #Check to see if the Value is an integer.  If it is not, force val_holder to be 0
                                try:
                                    #Variable to hold value for calculation of VOCAB_raw_score
                                    var_holder = int(output_dict[question][index])
                                    subj_total += var_holder
                                except ValueError:
                                    var_holder = 0
                                    subj_total += var_holder
                            output_dict[new_value][index] = subj_total

                        # Doing similar method for Matrix Items
                        elif new_value in MATRIX_ITEMS:
                            try:
                                matrix_value = int(row[old_value]) -1
                            except ValueError:
                                matrix_value = row[old_value]
                            output_dict[new_value][index] = vocab_value
                        elif new_value == "MATRIX_raw_score":
                            subj_total = 0 
                            for item in MATRIX_ITEMS:
                                try:
                                    var_holder = int(output_dict[item][index])
                                    subj_total += var_holder
                                except ValueError:
                                    pass
                            output_dict[new_value][index] = subj_total


                else:
                    output_dict[new_value] = input_csv[old_value]

            except KeyError:
                output_dict[new_value] = "ERROR"
                print(f"WARNING: Could not find {old_value} matching data for {new_value}")

        df = pd.DataFrame.from_dict(output_dict)
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

    