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
import logging
import subprocess
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def connect_to_orgclassifier():
    """
    Connects to the orgtype_classifier API (localhost server) on port 8080
    """
    # os.system('open -a Terminal .')
    cmd = ['python server.py model.pkl.gz']
    p = subprocess.Popen(cmd, shell=True, cwd=r'orgtype-classifier', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("Connecting to orgtype-classifier...")
    time.sleep(3)
    print("...Done\n")
    time.sleep(1)
    return p


def get_input_args():
    """
    Assign arguments including defaults to pass to the python call

    :return: arguments variable for both directory and the data file
    """

    parser = argparse.ArgumentParser(description="Input data file name/loc")
    parser.add_argument('--dir', default='', type=str,
                        help="set the data directory")
    parser.add_argument('--datafile', default='sample_orgs.csv', type=str,
                        help="set the data file")
    args = parser.parse_args()
    return args


def load_df(data_dir, data_file):
    """
    Load data file

    :param data_dir: the directory containing the datafile
        (default: current location)
    :param data_file: the csv file containing organisation information

    :return df: pandas dataframe
    :return df_name: name of df
    """
    df = pd.read_csv(str(data_dir + data_file))
    df_name = str(data_file)[:-4]
    # Dedupe won't run fully if dataframe not big enough (i.e. df[:5] won't work)
    df = df[:10]
    return df, df_name


def pre_processing(df):
    """
    Simple pre-processing function to:
    - Clarify the name of the column containing the organisation name,
    - Conversion of name column to type string,
    - Checks to see if there are blank rows and give option to remove
        them or quit

    :param df: the pandas dataframe

    :return: df
    """
    print("Data Sample: ")
    print(df.head(3))
    # String_col used in post_processing too therefore initiate globally
    global string_col
    string_col = str(input("\nWhat is the exact name of the column containing the organisation name? \n \
        (default is 'org_string' - hit enter for this input): \n") or 'org_string')
    time.sleep(0.5)

    while True:
        try:
            print("Converting org_string to string type...")
            df[string_col] = df[string_col].astype(str)

        except KeyError:
            string_col = str(input("Incorrect organisation name column entered, \
             try again :"))
            continue
        break
    time.sleep(0.5)
    print("...done")
    time.sleep(0.5)
    nans = lambda df: df.loc[df['org_string'].isnull()]
    print("\nThere are {} blank org_strings in the file".format(len(nans(df))))
    time.sleep(0.5)
    if len(nans(df)) > 0:
        print(nans(df))
        choice = input("Type 'y' to delete blank rows or 'n' to \
            quit and self-amend :")
        if choice == 'y':
            df = df.dropna(inplace=True)
        else:
            sys.exit()
    time.sleep(0.5)
    print("\nProgressing to org classification")
    return df


def classify_org(df):
    """
    Pass org_strings array to orgtype-classifier API to get the org_type

    :param df: the pandas dataframe

    :return orgtype_dict : dictionary containing the org_string and org_type
    """

    org_strings = df['org_string']
    orgtype_dict = {}
    # orgtype_classifier API accepts 50 strings max per request
    if len(org_strings) >= 50:
        split_length = 50
    else:
        split_length = 1

    for sub_arr in np.array_split(org_strings, split_length):
        concat_string = []
        for word in sub_arr:
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
        orgtype_dict.update(rj)

    return orgtype_dict


def map_columns(df):
    """
    - Call classify_org function
        and map results to new column org_type in dataframe
    - Dictionary comp_or_not_dict converts
        the org_type values to a condensed company vs not a company
        classification and is mapped to 'company_or_not'

    :param df: pandas dataframe

    :return df: pandas dataframe
    """

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

    orgtype_dict = classify_org(df)
    df['org_type'] = df['org_string'].map(orgtype_dict)
    df['company_or_not'] = df['org_type'].map(comp_or_not_dict)
    
    return df


def get_org_id(df):
    """
    Lookup company name via Companies House API and return company number

    :param df: pandas dataframe containing the organisation name

    :return df: Amended dataframe containing additional company information
    """

    s = chwrapper.Search(access_token=config.api_key)
    org_strings = df['org_string']
    org_id = {}
    chunk_propn = 0

    # Split org_string array into multiple arrays.
    # api states max batch of 600...
    # array_split doesn't have to have equal batch sizes.
    for chunk in np.array_split(org_strings,
                                math.ceil(len(org_strings) / 600), axis=0):

        print("\nProcessing companies house batch of size: " + str(len(chunk)))

        # For each org_string in the sub-array of org_strings
        # pull org data from companies house
        for word in chunk:
            response = s.search_companies(word)
            if response.status_code == 200:
                comp_house_dict = response.json()
                # response.json() returns a nested dict with complete org info
                # Below pulls just the company number.
                org_id[word] = comp_house_dict['items'][0]['company_number']
                org_id.update(org_id)
            elif response.status_code == 404:
                logger.debug("Error requesting CH data: %s %s",
                             response.status_code, response.reason)
            elif response.status_code == 429:
                logger.debug("Error requesting CH data: %s %s",
                             response.status_code, response.reason)
                logger.debug("Waiting...")
                time.sleep(60)
                s = chwrapper.Search(access_token=config.api_key)
            else:
                logger.error("Error requesting CH data: %s %s",
                             response.status_code, response.reason)
        chunk_propn += int(len(chunk))
        time.sleep(0.5)
        print("\nProgress: " + str(chunk_propn) + " of " +
              str(len(org_strings)))
        df['obtained_id'] = df['org_string'].map(org_id)
    return df

#     while True:
#         try:
#             # If obtained_id already exists (i.e previous runtime crashed, but saved progress)
#             # Filter the df for only the blank obtained_id rows
#             if 'obtained_id' in df:
#             # Filter df for blank ids
                
#                 isnull = df.obtained_id.isnull()
#                 notnull = df.obtained_id.notnull()
#                 empty_id_df = df[isnull]
#                 filled_id_df = df[notnull]
#                 org_strings = empty_id_df['org_string']
#                 partial_org_id = call_api(org_strings)
#                 # pdb.set_trace()
#                 # Below returns A value is trying to be set on a copy of a slice from a DataFrame.
# # Try using .loc[row_indexer,col_indexer] = value instead
#                 empty_id_df['obtained_id'] = empty_id_df['org_string'].map(partial_org_id)
#                 df_merged = pd.concat([filled_id_df, empty_id_df])
#                 # ***********code moves to except after the below:
#                 df_merged.to_csv(df_name + '_partial.csv')
#                 return df_merged
#             else:
#                 org_id = call_api(org_strings)
#                 df['obtained_id'] = df['org_string'].map(org_id)
#                 save_adjusted_data(df, df_name)
#                 return df

#         except urllib.error.HTTPError:
#             print("\nSleeping as close to batch limit")  
#             time.sleep(5 * 60)
#             # # Save progress after each chunk
#             #     save_adjusted_data(df, df_name)
#             continue
#         break

#         # Companies House API only allows up to 600 requests per 5 mins.
#         # If the batch wasn't the last batch, wait for 5 mins, then loop back to grab next chunk
#             # if chunk_propn < len(org_strings):
#             #     print("\nSleeping as close to batch limit")  
#             #     time.sleep(5 * 60)
#     return df


def post_processing(df, df_name):
    """
    - Check sample of adjusted dataframe
    - Check for blank company id's
    - User inputs pre-analysed id column if exists
    - Check against this to check matches

    :param df: pandas dataframe
    :param df_name: name of dataframe for saving purposes

    :return df
    """
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
                df[id_comparison] = df[id_comparison].str.lstrip("0")
                df['obtained_id'] = df['obtained_id'].str.lstrip("0")
            except KeyError:
                id_comparison = str(input("\nIncorrect id column name entered, try again :"))
                continue
            break

        df['id_mismatch'] = df[id_comparison] != df['obtained_id']

        print("\nThere is/are {} mis-matching ids in the file.\n"
              .format(sum(df['id_mismatch'])))
        time.sleep(0.5)

        # If there are mis-matching IDs, print a condensed table and
        # save to separate file for further investigation
        # Prevents having to re-run each time just to see the errors
        if (sum(df['id_mismatch'])) > 0:
            df_errors = df[df['id_mismatch']==True]
            print(df_errors[[string_col, id_comparison, 'obtained_id']])
            time.sleep(0.5)
            print("\nSaving mismatching ids to : " + str(df_name) +
                  '_classified_errors.csv')
            df_errors.to_csv(df_name + '_classified_errors.csv')
            time.sleep(0.5)
    return df

# def get_csvdedupe_args(df_name):
#     """
#     Assign arguments including defaults to pass to csvdedupe

#     :return: arguments variable for field_names and deduped output file
#     """
#     # string_col = 'org_string'
#     parser = argparse.ArgumentParser(description="Input field names/output filename")
#     # parser.add_argument('--input_file', default='sample_orgs_classified.csv')
                        
#     parser.add_argument('--field_names', default='org_string', type=str,
#                         help="set the columns to determine duplicates")
#     parser.add_argument('--output_file', default=df_name + "_deduped.csv", type=str,
#                         help="set the deduped output filename")
#     dedupe_args = parser.parse_args()
#     return dedupe_args


def deduplicate(infile, string, output_file):
    # pdb.set_trace()
    cmd = ['csvdedupe ' + infile + ' --field_names ' + str(string) + ' obtained_id' + ' --output_file ' + \
           str(output_file)]
    p = subprocess.Popen(cmd, shell=True)
    p.wait()


def save_adjusted_data(df, name):
    """
    Save adjusted dataframe to 'filename + _classified.csv'

    :param df
    :param name : name of dataframe

    :return None
    """
    print("\nSaving main output to : " + str(name) + '_classified.csv')
    df.to_csv(name + '_classified.csv')
    df_name = name + "_classified.csv"
    return df, df_name


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
    connect_to_orgclassifier()
    in_arg = get_input_args()
    df, df_name = load_df(in_arg.dir, in_arg.datafile)
    pre_processing(df)
    df = map_columns(df)
    df = get_org_id(df)
    save_adjusted_data(df, df_name)
    df = post_processing(df, df_name)
    classd_df, classd_name = save_adjusted_data(df, df_name)
    # dedupe_args = get_csvdedupe_args(df_name)
    deduplicate(classd_name, string_col, df_name + '_deduped')
