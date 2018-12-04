import pandas as pd
import os
import argparse
import time
import pdb
import math
import sys
import subprocess
from tqdm import tqdm
import sqlite3
import re
from pathlib import Path
pd.options.mode.chained_assignment = None  # default='warn'


def get_input_args():
    """
    Assign arguments including defaults to pass to the python call

    :return: arguments variable for both directory and the data file
    """
    pdb.set_trace()
    parser = argparse.ArgumentParser(description="Input data file name/loc")
    parser.add_argument('--dir', default='Data_Projects/Italian_Data/', type=str,
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


def deduplicate(infile, string, output_file):
    """
    Calls the dedupe.io api to cluster possible duplicates together.
    Outputs updated datafile with cluster id and confidence score
    (% chance that the row in question belongs to that cluster)

    :param infile: the already-classified datafile
    :param string: the user-defined org_string column name (default org_string)
    :param output_file: the deduped filename
    """
    # use pathlib to obtain the parent directory.
    homedir = Path(__file__).resolve().parents[0]
    data_fp = str(homedir) + "/" + str(in_arg.dir)

    # Module will look in the data directory for dedupe training/learned_settings files. If not, will create them there.
    training_fp = data_fp + "training.json"
    settings_fp = data_fp + "learned_settings"
    cmd = ['python csvdedupe.py ' + infile + ' --field_names ' + str(string) +
            ' address obtd_id obtained_address obtd_legal_name --output_file ' + str(output_file) + ' --training_file ' + str(training_fp) + ' --settings_file ' + str(settings_fp)]
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
    
    df = pd.read_csv(str(data_dir + df_name))
    Y = int(input("\nFor confidence scores >70%, select str length above which no further investigation is necessary (def=3):") or 3)
    df_70Y_accept = df[(df['Confidence Score']>=0.7) & (df[string_col].str.len() >= Y)]
    # df_70Y_accept = df_70Y_accept.drop(df_70Y_accept.columns[0], axis = 1)
    # ~ = opposite of what follows (i.e. is not in)
    df_70Y_unaccept = df[~df[string_col].isin(df_70Y_accept[string_col])]
    # df_70Y_unaccept = df_70Y_unaccept.drop(df_70Y_unaccept.columns[0], axis = 1)
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



def connect_SQL(db_path):
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
    results = cur.fetchall()
    return results

def add_info(results, df):
    '''
    Maps the returned results from the SQL to the relevant columns in the dataframe.

    :param results: the results list returned from sql_query()
    :param/return df: the dataframe
    '''
    org_dict = {}
    for word in results:
        org_dict[word[0]] = word[1:]
        org_dict.update(org_dict)
    df['results'] = df['org_string'].map(org_dict)
    for line in range(len(df['results'])):
        if pd.isnull(df['results'][line]):
            df['results'][line] = (math.nan, math.nan, math.nan)
    df[['obtained_address', 'obtd_id', 'obtd_legal_name', 'obtd_tax_id']] = pd.DataFrame(df['results'].values.tolist())
    # Copy the obtained id column as will be making adjustments in assign_org_ids_to_clusters
    df['obtained_id_orig'] = df['obtd_id']
    return df

def assign_org_ids_to_clusters(df, df_name):
    '''
    For members of a cluster with a confidence score greater than 70%, 
    they will be assigned the obtained id number of the highest-confidence row in that cluster.
    '''
    # df, df_name = load_df(indir, datafile[:-4] + '_ddup.csv')
    st = set(df['Cluster ID'])
    for idx in st:
        
        # filter df by the cluster id
        dfidx = df[df['Cluster ID'] == idx]
        # once the filter is applied filter again for non-blank obtd_id
        try:
            dfidx = pd.notnull(df)
            # then choose the one with the highest confidence
            max_conf_idx = dfidx['Confidence Score'].idxmax()
            # apply that obtd_id to the cluster members with a confidence > 70%
            for row in range(len(df)):
                try:
                    if df['Cluster ID'][row] == idx:
                        # If confidence is above 70%:
                        if df['Confidence Score'][row] >= 0.7:
                            # Update location row:obtd_id with the id pertaining to the highest confidence score in the cluster.
                            df.at[row, 'obtd_id' ] = int(df['obtd_id'][max_conf_idx])
                except:
                    continue
        except:
            continue
        # Sort rows by cluster     
        df = df.sort_values(by=['Cluster ID'])
        # Round confidence scores to 2dp. Can't format as % this converts to str, and need to compare to the threshold in confidence_processing()
        df['Confidence Score'] = df['Confidence Score'].map(lambda x: round(x, 2))
        # Save df
        df_name = save_data(in_arg.dir, df, df_name, '_idexpanded')
        return df, df_name


def file_tidy(df, joined_file=None):   
    '''
    Cleans up the output from deduping the file - dedupe adds duplicate id columns, etc.
    :param df: the pandas dataframe
    :param joined_file: the initial sql-joined & matched file

    :returns df
    ''' 
    # results column not needed
    df = df.drop('results', axis=1)
    # remove ('unnamed: 2') id column as duplicated. flags uses regex package to ignore case formatting.
    df = df.drop(df.columns[df.columns.str.contains('unnamed', flags=re.IGNORECASE)], axis = 1)
    # delete _merge.csv file
    if joined_file:
            os.remove(in_arg.dir + joined_file)

    # Can add in a yield or something here to make the function continue after confidence_processing has run.
    
    return df

# Some org_strings have detailed info in them i.e. ati costituita da
# ---------------------------------------------------------------
if __name__ == '__main__':
    in_arg = get_input_args()
    df, df_name = load_df(in_arg.dir, in_arg.datafile)
    # # # # # # # pre_processing(df)
    DEFAULT_PATH = os.path.join(os.path.dirname(__file__), in_arg.dir + 'ITA_db.db')
    con = connect_SQL(DEFAULT_PATH)
    results = sql_query(con)
    df = add_info(results, df)
    # # # # # # # df = post_processing(df, df_name)
    joined_file = save_data(in_arg.dir, df, df_name, '_merged')
    pdb.set_trace()
    deduplicate('../../' + in_arg.dir + joined_file, 'org_string' , '../../' + in_arg.dir + df_name + '_ddup.csv')
    df, df_name = load_df(in_arg.dir, in_arg.datafile[:-4] + '_ddup.csv')
    df = file_tidy(df, joined_file=None)
    # df, df_name = assign_org_ids_to_clusters(in_arg.dir, in_arg.datafile)
    df, df_name = assign_org_ids_to_clusters(df, df_name)
    df = confidence_processing(in_arg.dir, df_name, 'org_string')

    # To run and allow pdb to catch any error and enter debug mode :
    # python -m pdb -c continue DM_ITA_orgtype_classifierv5.py
