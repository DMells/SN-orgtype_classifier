import pandas as pd
import pytest
import requests
import pandas.api.types as ptypes
import os
import argparse
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


def classify_org(df):
    org_strings = df['org_string']
    orgtype_dict = {}

    # Split org_string column into chunks of 100 strings
    for chunk in np.array_split(org_strings, 100):
        concat_string = []
        for word in chunk:
            concat_string.append(word)
        concat_string = '&q='.join(concat_string)
        url = r'http://localhost:8080/predict?q=' + str(concat_string)
        r = requests.get(url)
        # Convert requests response object to python dict
        rj = r.json()

        # Merge sub-dict with master dictionary
        orgtype_dict.update(rj)

    return orgtype_dict


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
    orgtype_dict = classify_org(df)
    # Create org_type column which maps the values of dict to original strings
    df['org_type'] = df['org_string'].map(orgtype_dict)
    # Create comp or not column mapping the values of comp_or_not to the
    # org_type
    df['company_or_not'] = df['org_type'].map(comp_or_not_dict)
    return df

# 600 requests every 5 minutes
# def get_org_id(org_string):

#     org_id = {}
#     # row = df.loc[df['org_string'] ==  org_string]
#     # if row['org_type'] == ''
#     # if the org_type associated with the org_string = company, use comp house api:
#     # if df[df['org_type'].match(df['']]

#     # EXPORT API KEY TO ENVIRONMENT VARIABLE!!!!!
#     s = chwrapper.Search(access_token="ZBON8JY79yCOhfXp6SgRPNHsvLoAaUcpDtwXrqft")
#     r = s.search_companies(org_string)
#     comp_house_dict = r.json()
#     org_id[org_string] = comp_house_dict['items'][0]['company_number']

#     # if org_type is charity, use charity register:

#     return org_id

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
    df = map_columns(df)
    save_adjusted_data(df, df_name)
