# SN-orgtype_classifier

A Python module for verifying manually curated organisation data with government registers (eg UK companies house) or other such official data, 
and then fuzzy matching the data to link variations in eg: organisation name together by way of clustering.

The module makes use of dedupe, which uses machine learning (taught based on human input) to cluster together similar records, thus alleviating the need to analyse duplicate information.

Using the terminal the user can customise various elements of the analysis, such as the column names and confidence scoring criteria.

1. Clone the repository
-TODO : csvdedupe repo has been adjusted by me - need to ensure that the repo also clones the adjustments
2. Install requirements:

```
pip install -r requirements.txt
```

3. Navigate to the folder containing the main analysis file "DM_XX_match_MASTER.py"

4. Run command:
```
python DM_XX_match_MASTER.py
```

4.1 The module makes use of argument_parsing - i.e. to choose a different folder from the current one (default) containing organisation data, to (4) add `--dir '<foldername>'`. Likewise to change the default datafile (sample_orgs.csv) use `--datafile '<filename>'`

5. Follow terminal instructions 

6. Review various datafile outputs for manual intervention