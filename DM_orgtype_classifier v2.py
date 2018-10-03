import pandas as pd
import pytest
import requests
import pandas.api.types as ptypes
import os
import argparse
import json
import numpy as np



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
    return df, df_name


# Obtain org_type from classifier
# def classify_org(org_string_list):
#     concat_string = []
#     # orgtype_dict = {}
#     word_count = 0
#     if word_count <= 100:
#         for word in org_string_list:
#             concat_string.append(word)
#             word_count += 1
#         concat_string ='&q='.join(concat_string)
#     print(concat_string)
#     url = r'http://localhost:8080/predict?q=' + str(concat_string)
#     r = requests.get(url)
#     orgtype_dict = r.json()
#     return orgtype_dict
# ABOVE WORKS FINE FOR TEST LIST
# BELOW IS DUPLICATE BUT NEED TO BE ABLE TO MERGE DICTIONARIES TOGETHER
def classify_org(org_string_list):
    print(org_string_list)
    orgtype_dict = {}
    for chunk in np.array_split(org_string_list, 100):
        concat_string = []
        for word in chunk:
            concat_string.append(word)
        concat_string ='&q='.join(concat_string)
        url = r'http://localhost:8080/predict?q=' + str(concat_string)
        r = requests.get(url)
        # rj = r.json()
        # orgtype_dict.update(rj)
    return orgtype_dict


def classify_org(org_string_list):
    print(org_string_list)
    orgtype_dict = {}
    for word in org_string_list:
        url = r'http://localhost:8080/predict?q=' + str(word)
        r = requests.get(url)
        rj = r.json()
        orgtype_dict.update(rj)
    return orgtype_dict


# org = classify_org(list)

# Determine if org_type is company or not (note : assumed that
# "Community Interest Company" is not a company)
# def company_or_not(org_type):
#     org_type_dict = {'Private Limited Company': 'Company',
#                      'Company Limited by Guarantee': 'Company',
#                      'Royal Charter Company': 'Company',
#                      'Community Interest Company': 'Not A Company',
#                      'Registered Society': 'Not A Company',
#                      'Registered charity': 'Not A Company',
#                      'Individual': 'Not A Company',
#                      'Government': 'Not A Company',
#                      'School': 'Not A Company',
#                      'Community Amateur Sports Club': 'Not A Company',
#                      'Local Authority': 'Not A Company',
#                      'Parish or Town Council': 'Not A Company'}

#     company_or_not = org_type_dict[org_type]
#     return company_or_not


# Apply org_type and company_or_not functions to dataframe
def apply_columns(df):
    df['org_type'] = df['org_string'].apply(classify_org)
    # df['company_or_not'] = df['org_type'].apply(company_or_not)
    return df    


    


# Save dataframe as CSV under amended file name
def save_adjusted_data(df, name):
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
    df = apply_columns(df)
    save_adjusted_data(df, df_name)
