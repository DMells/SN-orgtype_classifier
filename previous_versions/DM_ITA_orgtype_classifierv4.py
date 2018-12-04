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
import sqlite3
# import json
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

pd.options.mode.chained_assignment = None  # default='warn'


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
            ' address obtained_id obtained_address obtained_legal_name --output_file ' + str(output_file)]
    p = subprocess.Popen(cmd, cwd='./csvdedupe/csvdedupe', shell=True)
    p.wait()


def confidence_processing(data_dir, df_name, string_col):
    '''
    Split deduped dataframe twice. One is for deduped rows >70% confidence
    score AND no. of letters > Y. This is because a deviation for a string of
    3 letters will likely mean it's a totally different company. Second is for
    all other data.

    :param data_dir: filepath to folder containing deduped data
    :param df_name: name of dataframe
    :string_col: user-defined name for the column containing the org_strings

    :return df_70Y_accept_name: name of df with >70% & > Y length strings
    :return df_70Y_unaccept_name: name of df with <70% or
    (>70% AND Y length strings)
    '''
    df = pd.read_csv(str(data_dir + df_name + '_ddup.csv'))
    Y = int(input("\nFor confidence scores of >70%, select the string-length below which no further investigation is deemed necessary (default 3):") or 3)
    df_70Y_accept = df[(df['Confidence Score']>=0.7) & (df[string_col].str.len() >= Y)]
    df_70Y_unaccept = df[~df[string_col].isin(df_70Y_accept[string_col])]
    print("Splitting dataframes based on confidence/letter criteria...")
    print("...Done")
    # Save data which we totally accept as being matched.
    df_70Y_accept_name = save_data(data_dir, df_70Y_accept, df_name, '_accept' )
    # Save residual data which we aren't confident about.
    df_70Y_unaccept_name = save_data(data_dir, df_70Y_unaccept, df_name, '_unaccept' )
    return df_70Y_accept_name, df_70Y_unaccept_name

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

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'ITA_db.db')

def connect_SQL(db_path=DEFAULT_PATH):
    '''
    Establishes connection to specified SQL database
    :param db_path: - path to db file

    :return con: connection to the database
    '''
    con = sqlite3.connect(db_path)
    return con


def sql_query(con):
    """
    Execute SQL query which joins the two data files together
    :file sd_abc: represents the 'truth' i.e. perhaps government/official data
    :file italian_suppliers_abc: the manually curated data to which we wish to add official company data and eventually dedupe.

    :return results: query results in list format. Each element is a tuple containing strings -  org name, address, id, legal name and tax id.
    """
    cur = con.cursor()
    org_id_matches = """
    SELECT 
        italian_suppliers_abc.org_string,
        italian_suppliers_abc.address,
        sd_abc.id,
        sd_abc.legal_name,
        sd_abc.tax_id

    FROM
        italian_suppliers_abc

    LEFT JOIN sd_abc ON italian_suppliers_abc.org_string == sd_abc.company_name

    WHERE
        sd_abc.id NOT NULL;
    """ 

    cur.execute(org_id_matches)
    pdb.set_trace()
    results = cur.fetchall()
    return results

def add_info(results, df):
    '''
    Maps the returned results from the SQL to the relevant columns in the dataframe.
    '''
    org_dict = {}
    for word in results:
        org_dict[word[0]] = word[1:]
        org_dict.update(org_dict)
    # pdb.set_trace()
    df['results'] = df['org_string'].map(org_dict)
    for line in range(len(df['results'])):
        if pd.isnull(df['results'][line]):
            df['results'][line] = (math.nan, math.nan, math.nan)
    df[['obtained_address', 'obtained_id', 'obtained_legal_name', 'obtained_tax_id']] = pd.DataFrame(df['results'].values.tolist())
    # Copy the obtained id column as will be making adjustments in assign_org_ids_to_clusters
    df['obtained_id_orig'] = df['obtained_id']
    # results column not needed
    # df = df.drop(['results', 'count', 'Unnamed: 2'], axis=1)

    # Copy long org strings into address column so dedupe has a chance to match addresses. - NO NEED
    # for line in range(len(df['org_string'])):
    #     if len(df['org_string'][line]) >= 100:
    #         # pdb.set_trace()
    #         if pd.isnull(df['address'][line]):
    #             df['address'][line] = df['org_string'][line]
    # pdb.set_trace()
    # mask1 = df.org_string.str.len() >= 100
    # mask2 = df.address.str.len().isnull()

    # df = df.assign(address=df.org_string.where(mask1 & mask2, df.address))
    return df

def assign_org_ids_to_clusters(indir, datafile):
    '''
    For members of a cluster with a confidence score greater than 70%, 
    they will be assigned the obtained id number of the highest-confidence row in that cluster.
    '''
    df, df_name = load_df(indir, datafile[:-4] + '_deduped.csv')
    # df = df[df['Cluster ID'] == 6]
    st = set(df['Cluster ID'])
    for idx in st:
        
        # filter df by the cluster id
        dfidx = df[df['Cluster ID'] == idx]
        # once the filter is applied filter again for non-blank obtained_id
        try:
        # dfidx = dfidx[pd.notnull(dfidx['obtained_id'])]
            dfidx = pd.notnull(df)
            # then choose the one with the highest confidence
            max_conf_idx = dfidx['Confidence Score'].idxmax()
            # apply that obtained_id to the cluster members with a confidence > 70%
            for row in range(len(df)):
                try:
                    if df['Cluster ID'][row] == idx:
                        # If  confidence is above 70%:
                        if df['Confidence Score'][row] >= 0.7:
                            # Update location row:obtained_id with the id pertaining to the highest confidence score in the cluster.
                            df.at[row, 'obtained_id' ] = int(df['obtained_id'][max_conf_idx])
                except:
                    continue
        except:
            continue
        # Sort rows by cluster     
        df = df.sort_values(by=['Cluster ID'])
        # Format confidence scores as %
        df['Confidence Score'] = df['Confidence Score'].map(lambda x: '{:.2%}'.format(x))
        # Save df
        df_name = save_data(in_arg.dir, df, df_name, '_idexpanded')
        return df, df_name



# After dedupe - should merge obtained data within all clusters (maybe only above a certain confidence score, etc).
# Some org_strings have detailed info in them i.e. ati costituita da
# ---------------------------------------------------------------
if __name__ == '__main__':
    in_arg = get_input_args()
    df, df_name = load_df(in_arg.dir, in_arg.datafile)
    # # # pre_processing(df)
    con = connect_SQL()
    results = sql_query(con)
    df = add_info(results, df)
    # # # df = post_processing(df, df_name)
    # classd_name = save_data(in_arg.dir, df, df_name, '_classified')
    df = deduplicate('../../' + in_arg.dir + df_name, 'org_string','../../' + in_arg.dir + df_name + '_ddup.csv')
    df = assign_org_ids_to_clusters(in_arg.dir, in_arg.datafile)
    df = confidence_processing(in_arg.dir, df_name, 'org_string')
    # To run and allow pdb to catch any error and enter debug mode :
    # python -m pdb -c continue DM_ITA_orgtype_classifierv4.py
