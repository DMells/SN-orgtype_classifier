import pandas as pd
import requests
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
from tqdm import tqdm
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def connect_to_orgclassifier():
    """
    Connects to the orgtype_classifier API (localhost server) on port 8080
    """
    # os.system('open -a Terminal .')
    cmd = ['python server.py model.pkl.gz']
    p = subprocess.Popen(cmd, shell=True, cwd=r'orgtype-classifier',
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    df = df[:200]

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
        (default is 'org_string' - hit enter for this input): \n") or
                     'org_string')
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

    for sub_arr in tqdm(np.array_split(org_strings, split_length)):
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
    ch_org_dict = {}
    chunk_propn = 0

    # Split org_string array into multiple arrays.
    # api states max batch of 600...
    # array_split doesn't have to have equal batch sizes.
    for chunk in np.array_split(org_strings,
                                math.ceil(len(org_strings) / 100), axis=0):

        print("\nProcessing companies house batch of size: " + str(len(chunk)))

        # For each org_string in the sub-array of org_strings
        # pull org data from companies house
        for word in tqdm(chunk):
            response = s.search_companies(word)
            if response.status_code == 200:
                comp_house_dict = response.json()
                # response.json() returns a nested dict with complete org info
                # Below pulls just the company number.
                # pdb.set_trace()
                ch_org_dict[word] = [comp_house_dict['items'][0]
                                    ['company_number']]
                # Pull through address and incorporation date
                try:
                    address = comp_house_dict['items'][0]['address_snippet']
                except:
                    address = str('None')

                try:
                    inc_date = comp_house_dict['items'][0]['date_of_creation']
                except:
                    inc_date = '1000-01-01'

                ch_org_dict[word].extend([address, inc_date])
                ch_org_dict.update(ch_org_dict)

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
        # pdb.set_trace()
        df['obtained_id'] = df['org_string'].map(ch_org_dict)
    try:
        df[['obtained_id', 'address', 'incorporation_date']] = \
            pd.DataFrame(df['obtained_id'].values.tolist())
    except KeyError as e:
        print(e.message)
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
    preset_id = str(input("\nIs there a column in the dataset representing \
        already-analysed company numbers? (y/n) :"))
    if preset_id == 'y':
        id_comparison = str(input("\nPlease enter the name of the comparative\
         column :\n"))
        time.sleep(0.5)
        while True:
            try:
                print("\nComparing obtained_ids to pre-analysed ids...")
                df[id_comparison] = df[id_comparison].str.lstrip("0")
                df['obtained_id'] = df['obtained_id'].str.lstrip("0")
            except KeyError:
                id_comparison = str(input("\nIncorrect id column name entered, \
                    try again :"))
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
    Y = int(input("\nFor confidence scores of >90%, select the string-length\
     below which no further investigation is deemed necessary \
     (default 3):") or 3)
    df_90Y_accept = df[(df['Confidence Score'] >= 0.9) & (df[string_col]
                                                          .str.len() >= Y)]
    df_90Y_unaccept = df[~df[string_col].isin(df_90Y_accept[string_col])]
    time.sleep(0.5)
    print("Splitting dataframes based on confidence/letter criteria...")
    time.sleep(0.5)
    print("...Done")
    df_90Y_accept_name = save_data(data_dir, df_90Y_accept, df_name,
                                   '_accepted_conf')
    time.sleep(0.5)
    df_90Y_unaccept_name = save_data(data_dir, df_90Y_unaccept,
                                     df_name, '_unaccepted_conf')
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


# ---------------------------------------------------------------
if __name__ == '__main__':
    connect_to_orgclassifier()
    in_arg = get_input_args()
    df, df_name = load_df(in_arg.dir, in_arg.datafile)
    pre_processing(df)
    df = map_columns(df)
    df = get_org_id(df)
    df = post_processing(df, df_name)
    classd_name = save_data(in_arg.dir, df, df_name, '_classified')
    deduplicate('../../' + classd_name, string_col, '../../' +
                df_name + '_deduped.csv')
    confidence_processing(in_arg.dir, df_name, string_col)

    # To run and allow pdb to catch any error and enter debug mode :
    # python -m pdb -c continue DM_orgtype_classifier_v15.py
