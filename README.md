# SN-orgtype_classifier

A Python module for verifying manually curated organisation data with government registers (eg UK companies house), 
and then fuzzy matching the data to link variations in eg: organisation name together by way of clustering.

Using the terminal the user can customise various elements of the analysis, such as the column names and confidence scoring criteria.

1. Clone the repository

2. Install requirements:

```
pip install -r requirements.txt
```

3. Navigate to the folder containing the main analysis file "DM_orgtype_classifier_vXX.py"

4. Run command:
```
python DM_orgtype_classifier_vXX.py
```

4.1 The module makes use of argument_parsing - i.e. to choose a different folder from the current one (default) containing organisation data, to (4) add `--dir '<foldername>'`. Likewise to change the default datafile (sample_orgs.csv) use `--datafile '<filename>'`

5. Follow terminal instructions 

6. Review various datafile outputs for manual intervention