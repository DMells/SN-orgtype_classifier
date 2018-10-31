import argparse
import logging
import pdb
import subprocess
import optparse
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


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


# def deduplicate(input_file, field_names, output_file):
#     cmd = ['csvdedupe ' + input_file, field_names, ' --output_file ', output_file]
     
#     p = subprocess.Popen(cmd, shell=True)
#     p.wait()
    

# def deduplicate():
#     cmd = ['csvdedupe sample_orgs_classified.csv --field_names org_string']
     
#     p = subprocess.Popen(cmd, shell=True)
#     p.wait()
    

def deduplicate(infile, string):
    cmd = ['csvdedupe ' + infile + ' --field_names ' + string]
    p = subprocess.Popen(cmd, shell=True)
    p.wait()


# ---------------------------------------------------------------
if __name__ == '__main__':
    df_name = 'sample_orgs'
    pdb.set_trace()
    infile = 'sample_orgs_classified.csv'
    org_string = 'org_string'
    # dedupe_args = get_csvdedupe_args(df_name, org_string)
    # deduplicate(infile, dedupe_args.field_names, dedupe_args.output_file)
    deduplicate(infile, org_string)
