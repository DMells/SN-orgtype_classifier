import pandas as pd
import pytest
import requests
import pandas.api.types as ptypes
import os
import argparse
import numpy as np
import chwrapper
import time
import pdb
import math
import config
import sys
from urllib2 import HTTPError

# Define input arguments for data file and directory customisation
def get_input_args():
    parser = argparse.ArgumentParser(description="Input data file name/loc")
    parser.add_argument('--dir', default='', type=str,
                        help="set the data directory")
    parser.add_argument('--datafile', default='sample_orgs.csv', type=str,
                        help="set the data file")
    args = parser.parse_args()
    return args


# Read in data to dataframe and assign name (for saved file purposes)
def load_df(data_dir, data_file):
    df = pd.read_csv(str(data_dir + data_file))
    df_name = str(data_file)[:-4]
    # df = df[0:10]
    return df, df_name

def pre_processing(df):
    print("Data Sample: ")
    print(df.head(3))
    global string_col
    string_col = str(input("\nWhat is the exact name of the column containing the organisation name? \n(without quotes, see above sample): \n"))
    time.sleep(0.5)
    
    # User inputs column with org strings in, with catch if incorrect name entered
    while True:
        try: 
            print("Converting org_string to string type...")
            df[string_col] = df[string_col].astype(str)

        except KeyError:
            string_col = str(input("Incorrect organisation name column entered, try again :"))
            continue
        break
    time.sleep(0.5)
    print("\n...done")
    time.sleep(0.5)
    nans = lambda df: df.loc[df['org_string'].isnull()]
    print("\nThere are {} blank org_strings in the file".format(len(nans(df))))
    time.sleep(0.5)
    if len(nans(df)) > 0:
        print(nans(df))
        choice = input("Type 'y' to delete blank rows or 'n' to quit and self-amend :")
        if choice == 'y':
            df = df.dropna(inplace=True)
        else:
            sys.exit()
    time.sleep(0.5)
    print("Progressing to org classification \n")


    # print("There are {} blanks" + df.isnull().values.any())

# Function passes org_strings array to orgtype-classifier API to get the org_type
def classify_org(df):
    org_strings = df['org_string']
    orgtype_dict = {}
    rj = {}
    # Split org_string column into chunks of 50 strings - orgtype-classifier accepts 50 strings max
    for chunk in np.array_split(org_strings, 2):
        concat_string = []
        for word in chunk:
            concat_string.append(word)
        concat_string = '&q='.join(concat_string)
        url = r'http://localhost:8080/predict?q=' + str(concat_string)
        r = requests.get(url)
        try:
        # Convert requests response object to python dict
            rj = r.json()
        except ValueError:
            print(ValueError)
            break
        # Merge sub-dict with master dictionary
        orgtype_dict.update(rj)
        
    return orgtype_dict, df


# Apply obtained org_type and company_or_not_dicts to dataframe
def map_columns(df):
    # Dictionary mapping org type to manual company vs not classification
    # (note : assumed that "Community Interest Company" is not a company)

    comp_or_not_dict = {'Private Limited Company': 'Company',
                        'Company Limited by Guarantee': 'Company',
                        'Royal Charter Company': 'Company',
                        'Community Interest Company': 'Not A Company',
                        'Registered Society': 'Not A Company',
                        'Registered charity': 'Not A Company',
                        'Individual': 'Not A Company',
                        'Government': 'Not A Company',
                        'School': 'Not A Company',
                        'Community Amateur Sports Club': 'Not A Company',
                        'Local Authority': 'Not A Company',
                        'Parish or Town Council': 'Not A Company'}

    # Get dictionary {org_string: org_type} passed to org_classifier
    orgtype_dict, df = classify_org(df)
    # Create org_type column which maps the values of dict to original strings
    df['org_type'] = df['org_string'].map(orgtype_dict)

    # Create comp or not column mapping the values of comp_or_not to the
    # org_type
    df['company_or_not'] = df['org_type'].map(comp_or_not_dict)
    org_id_dict = get_org_id(df)
    df['obtained_id'] = df['org_string'].map(org_id_dict)
    return df

    # MULTIPLE STRING SEARCH - NOT WORKING - ONLY RETURNS ONE STRING
# Function which calls to the Companies House API via chwrapper, passes in org_string column from df
# def get_org_id(df):
#     org_id = {}
#     # chunk_propn = 0
#     s = chwrapper.Search(access_token=config.api_key)
#     # org_strings = df['org_string']
#     org_strings = ['001 INSPIRATION LIMITED', '007 PEST CONTROL LTD']
#     print(org_strings)
#     # math.ceil(len(org_strings)/600)
#     # for chunk in np.array_split(org_strings, 50):
#     # print("Processing companies house batch of size: " + str(len(chunk)))
#     r = s.search_companies(list(org_strings))
#     print(r.text)
#     comp_house_dict = r.json()
#     # For each org_string in the sub-array of org_strings, pull org data from companies house
#     for word in org_strings:
#         for i in range(0, len(comp_house_dict['items']), 1):
#             org_title = str(comp_house_dict['items'][i]['title'])
#             basic_string_comph = org_title.replace("  ", " ")
#             basic_string_df = word.replace("  ", " ")
#             if basic_string_comph == basic_string_df:
#                 print("MATCH")
#                 org_id[word] = comp_house_dict['items'][i]['company_number']
#                 org_id.update(org_id)

#         #  # Print progress
#         # chunk_propn += int(len(chunk))
#         # print("Progress: " + str(chunk_propn) + " of " + str(len(org_strings)))

# #         # Companies House API only allows up to 600 requests per 5 mins.
# #         # If the batch wasn't the last batch, wait for 5 mins, then loop back to grab next chunk
#         # if chunk_propn < len(org_strings):
#         #     print("Sleeping as close to batch limit")  
#         #     # time.sleep(5 * 60)
#     print(org_id)
#     return org_id

# SINGLE STRING SEARCH
# Function which calls to the Companies House API via chwrapper, passes in org_string column from df
def get_org_id(df):
    s = chwrapper.Search(access_token=config.api_key)
    org_strings = df['org_string']
    org_id = {}
    # chunk_propn = 0
    
    # Split org_string array into multiple arrays. Length of sub-array based on max batch size of 600 
    # rounded up. array_split doesn't have to have equal batch sizes
    for chunk in np.array_split(org_strings, math.ceil(len(org_strings)/600), axis=0):
        print("\nProcessing companies house batch of size: " + str(len(chunk)))
        # For each org_string in the sub-array of org_strings, pull org data from companies house
        for word in chunk:
            r = s.search_companies(word)
            comp_house_dict = r.json()
            # r returns a nested dict with complete info on the org. Below pulls just the ID number.
            org_id[word] = comp_house_dict['items'][0]['company_number']
            org_id.update(org_id)
        # Print progress
        # chunk_propn += int(len(chunk))
        # time.sleep(0.5)
        # print("\nProgress: " + str(chunk_propn) + " of " + str(len(org_strings)))

        # Companies House API only allows up to 600 requests per 5 mins.
        # # If the batch wasn't the last batch, wait for 5 mins, then loop back to grab next chunk
        # if chunk_propn < len(org_strings):
        #     print("\nSleeping as close to batch limit")  
        #     time.sleep(5 * 60)

    return org_id

def post_processing(df, df_name):
    time.sleep(1)
    print("\nPost processed data Sample: ")
    print(df.head(10))
    time.sleep(0.5)
    print("\nChecking org_id data: ")
    time.sleep(0.5)
    nans = lambda df: df.loc[df['obtained_id'].isnull()]
    print("\nThere are {} blank org_ids in the file.".format(len(nans(df))))
    if len(nans(df)) != 0:
        print(nans(df))
    preset_id = str(input("\nIs there a column in the dataset representing already-analysed company numbers? (y/n) :"))
    if preset_id == 'y':
        id_comparison = str(input("\nPlease enter the name of the comparative column :\n"))
        time.sleep(0.5)
        while True:
            try:
                print("\nComparing obtained_ids to pre-analysed ids...")
                df[id_comparison] = df[id_comparison].astype(int)
                df['obtained_id'] = df['obtained_id'].astype(int)
            except KeyError:
                id_comparison = str(input("\nIncorrect id column name entered, try again :"))
                continue
            break
  
        df['id_mismatch'] = df[id_comparison] != df['obtained_id']

        print("\nThere is/are {} mis-matching ids in the file.\n".format(sum(df['id_mismatch'])))
        time.sleep(0.5)

        # If there are mis-matching IDs, print a condensed table and save to separate file for further investigation
        # Prevents having to re-run each time just to see the errors
        if (sum(df['id_mismatch'])) > 0:
            df_errors = df[df['id_mismatch']==True]
            print(df_errors[[string_col, id_comparison, 'obtained_id']])
            time.sleep(0.5)
            print("\nSaving mismatching ids to : " + str(df_name) + '_classified_errors.csv')
            df_errors.to_csv(df_name + '_classified_errors.csv')
            time.sleep(0.5)
    return df
# Save dataframe as CSV under amended file name
def save_adjusted_data(df, name):
    print("\nSaving main output to : " + str(df_name) + '_classified.csv')
    df.to_csv(name + '_classified.csv')


# ---------------------------------TESTS--------------------------
def test_check_df_loaded():
    try:
        df, df_name = load_df('', 'sample_orgs.csv')
    except None:
        pytest.fail("Error loading df")


def test_checkinputisstring():
    df, df_name = load_df('', 'sample_orgs.csv')
    assert ptypes.is_string_dtype(df['org_string'])


def test_checknoblanks():
    df, df_name = load_df('', 'sample_orgs.csv')
    assert df['org_string'].isnull().values.any() is False


def test_checkconnectedtolocalhost():
    assert os.system("ping -c 1 localhost") == 0


# ---------------------------------------------------------------
if __name__ == '__main__':
    in_arg = get_input_args()
    df, df_name = load_df(in_arg.dir, in_arg.datafile)
    pre_processing(df)
    df = map_columns(df)
    df = post_processing(df, df_name)
    save_adjusted_data(df, df_name)


