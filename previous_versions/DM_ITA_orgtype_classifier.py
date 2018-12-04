import pandas as pd
import requests
import pandas.api.types as ptypes
import os
import argparse
import numpy as np
import time
import pdb
import math
import config
import sys
import logging
import subprocess
from tqdm import tqdm
# import sqlite3
import json
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def get_input_args():
    """
    Assign arguments including defaults to pass to the python call

    :return: arguments variable for both directory and the data file
    """

    parser = argparse.ArgumentParser(description="Input data file name/loc")
    parser.add_argument('--dir', default='Italian_Data/', type=str,
                        help="set the data directory")
    parser.add_argument('--datafile', default='italian_suppliers_abc.csv', type=str,
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
    # df = df[100:200]
    # df = df[:200]

    assert len(df) > 5    
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
            df_errors = df[df['id_mismatch'] == True]
            print(df_errors[[string_col, id_comparison, 'obtained_id']])
            time.sleep(0.5)
            print("\nSaving mismatching ids to : " + str(df_name) +
                  '_classified_errors.csv')
            df_errors.to_csv(df_name + '_classified_errors.csv')
            time.sleep(0.5)
    return df


def deduplicate(infile, string, output_file):
    """
    Calls the dedupe.io api to cluster possible duplicates together.
    Outputs updated datafile with cluster id and confidence score
    (% chance that the row in question belongs to that cluster)

    :param infile: the already-classified datafile
    :param string: the user-defined org_string column name (default org_string)
    :param output_file: the deduped filename
    """
    cmd = ['python csvdedupe.py ' + infile + ' --field_names ' + str(string) +
           ' obtained_id address incorporation_date' + ' --output_file ' +
           str(output_file)]
    p = subprocess.Popen(cmd, cwd='./csvdedupe/csvdedupe', shell=True)
    p.wait()


def confidence_processing(data_dir, df_name, string_col):
    '''
    Split deduped dataframe twice. One is for deduped rows >90% confidence
    score AND no. of letters > Y. This is because a deviation for a string of
    3 letters will likely mean it's a totally different company. Second is for
    all other data.

    :param data_dir: filepath to folder containing deduped data
    :param df_name: name of dataframe
    :string_col: user-defined name for the column containing the org_strings

    :return df_90Y_accept_name: name of df with >90% & > Y length strings
    :return df_90Y_unaccept_name: name of df with <90% or
    (>90% AND Y length strings)
    '''
    df = pd.read_csv(str(data_dir + df_name + '_deduped.csv'))
    Y = int(input("\nFor confidence scores of >90%, select the string-length below which no further investigation is deemed necessary (default 3):") or 3)
    df_90Y_accept = df[(df['Confidence Score']>=0.9) & (df[string_col].str.len() >= Y)]
    df_90Y_unaccept = df[~df[string_col].isin(df_90Y_accept[string_col])]
    time.sleep(0.5)
    print("Splitting dataframes based on confidence/letter criteria...")
    time.sleep(0.5)
    print("...Done")
    df_90Y_accept_name = save_data(data_dir, df_90Y_accept, df_name, '_accepted_conf' )
    time.sleep(0.5)
    df_90Y_unaccept_name = save_data(data_dir, df_90Y_unaccept, df_name, '_unaccepted_conf' )
    return df_90Y_accept_name, df_90Y_unaccept_name

def save_data(data_dir, df, df_name, suffix=None):
    """
    Save adjusted dataframe to 'filename + _classified.csv'
    :param data_dir: filepath to folder containing deduped data
    :param df
    :param name : name of dataframe
    :param suffix: ending of filename (i.e. _classified, _deduped etc)

    :return df_name
    """
    # pdb.set_trace()
    orig_file_name = data_dir + df_name
    if suffix:
        print("\nSaving output to : " + str(orig_file_name + suffix) + '.csv')
        df.to_csv(orig_file_name + suffix + '.csv')
        df_name += suffix + '.csv'
    else: 
        print("\nSaving output to : " + str(data_dir + df_name) + '.csv')
        df.to_csv(orig_file_name + '.csv')
        df_name += '.csv'
    return df_name

def load_ITA_json(dir):
    with open(str(dir) + 'sd_abc.json', encoding='utf-8') as F:
        json_data = json.loads(F.read())
    return json_data

def get_org_id(SNdf, ITA_df):
    """
    Lookup company name via SQL database and return company number
    :param df: pandas dataframe containing the organisation name
    :return df: Amended dataframe containing additional company information
    """
    org_dict = {}
    org_strings = SNdf['org_string']
    # pdb.set_trace()
    for word in tqdm(org_strings):
        for idx in range(len(ITA_df)):
            if word in ITA_df[idx]['company_name']:
                # pdb.set_trace()
                org_dict[word] = ITA_df[idx]['id']
            else:
                org_dict[word] = 0
    SNdf['obtained_id'] = SNdf['org_string'].map(org_dict)

    return SNdf
    

# ---------------------------------------------------------------
if __name__ == '__main__':
    in_arg = get_input_args()
    df, df_name = load_df(in_arg.dir, in_arg.datafile)
    # pre_processing(df)
    ITA_df = load_ITA_json(in_arg.dir)
    df = get_org_id(df, ITA_df)
    # df = post_processing(df, df_name)
    classd_name = save_data(in_arg.dir, df, df_name, '_classified')
    # deduplicate('../../' + classd_name, string_col,'../../' + df_name + '_deduped.csv')
    # confidence_processing(in_arg.dir, df_name, string_col)

    # To run and allow pdb to catch any error and enter debug mode :
    # python -m pdb -c continue DM_orgtype_classifier_v15.py
