import pandas as pd
import pytest
import requests
import pandas.api.types as ptypes
import os
import argparse
import numpy as np
import chwrapper
import time


# Define input arguments for data file and directory customisation
def get_input_args():
    parser = argparse.ArgumentParser(description="Input data file name/loc")
    parser.add_argument('--dir', default='', type=str,
                        help="set the data directory")
    parser.add_argument('--datafile', default='sample_orgs_classified.csv', type=str,
                        help="set the data file")
    args = parser.parse_args()
    return args


# Read in data to dataframe and assign name (for saved file purposes)
def load_df(data_dir, data_file):
    df = pd.read_csv(str(data_dir + data_file))
    df_name = str(data_file)[:-4]
    # df = df[:200]
    return df, df_name

# Apply obtained org_type and company_or_not_dicts to dataframe
def map_columns(df):
    org_id_dict = get_org_id(df)
    df['obtained_id'] = df['org_string'].map(org_id_dict)
    return df

def get_org_id(df):
    s = chwrapper.Search(access_token="ZBON8JY79yCOhfXp6SgRPNHsvLoAaUcpDtwXrqft")
    org_strings = df['org_string']
    org_id = {}
    for chunk in np.array_split(org_strings, 600):
        for word in chunk:
        # If the org string has an org type of company:
            r = s.search_companies(word)
            comp_house_dict = r.json()
            org_id[word] = comp_house_dict['items'][0]['company_number']
            org_id.update(org_id)
        # Comp house API only allows 600 requests every 5 mins:
        save_adjusted_data(df, df_name)
        # time.sleep(5 * 60)
    return org_id

# Save dataframe as CSV under amended file name
def save_adjusted_data(df, name):
    df.to_csv(name + '_idd.csv')


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
