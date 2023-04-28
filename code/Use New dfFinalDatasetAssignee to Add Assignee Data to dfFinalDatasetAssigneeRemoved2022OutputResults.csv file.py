import pandas as pd
import os
import time
import warnings
warnings.filterwarnings('ignore')

"""
Author: Joshua Chu
Date: April 23, 2023

Description: this script merges the newly created dfFinalDatasetAssigneeAddedAssignee.csv
file containing the assignee information and the dfFinalDatasetAssigneeRemoved2022OutputResults.csv
file, which is the input data provided to Mike. The goal is to append the latter data set with the
assignee information collected in the dfFinalDatasetAssigneeAddedAssignee.csv file from the
python script Prepare Patentview Data for Mike - Groupby Assignee Only.py. The resulting
output file is what will be applied to the Merge OpenCorporate Results with Input Data and
Clean for Scoring.py script when you process and merge the OC API results obtained from
Mike.

The output file should be utilized with the Merge OpenCorporate Results with Input Data and
Clean for Scoring.py script.

"""

### the section below imports the data set that incorporated the
### assignee information into the PatentsView data set
t0=time.time()

srcFiles="../csvResults/"

assignee=pd.read_csv(os.path.join(srcFiles,"dfFinalDatasetAssigneeAddedAssignee.csv"),
                     usecols=['assignee_id','organization','patent','assignee','assignor','record_date'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(assignee['assignee_id'].unique()))
print("The total number of unique patents are:",len(assignee['patent'].unique()),"\n")

print(assignee.info(null_counts=True),flush=True)
print(assignee.head(),flush=True)


### imports the previously used input file for the OC API
t0=time.time()

pvData=pd.read_csv(os.path.join(srcFiles,"dfFinalDatasetAssigneeRemoved2022OutputResults.csv"))
pvData.rename(columns={'assignee':'assignee_id'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(pvData['assignee_id'].unique()),"\n")

print(pvData.info(null_counts=True),flush=True)
print(pvData.head(),flush=True)


### merges both data sets utilizing the assignee_id and
### outer merge. Fields were renamed and selected
t0=time.time()

addAssignee=assignee.merge(pvData,on=['assignee_id'],how='outer',indicator=True)
addAssignee1=addAssignee.loc[addAssignee['_merge']=='both'].copy()
addAssignee2=addAssignee1.iloc[:,0:18]
addAssignee2.rename(columns={'organization_y':'organization'},inplace=True)

addAssignee3=addAssignee2.iloc[:,[6,0,7,8,9,10,11,12,13,14,15,16,2,17,3,4,5]]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(addAssignee3['assignee_id'].unique()))
print("The total number of unique patents are:",len(addAssignee3['patent'].unique()),"\n")

print(addAssignee3.info(null_counts=True),flush=True)
print(addAssignee3.head(),flush=True)

addAssignee3.to_csv("../csvResults/dfFinalDatasetAssigneeAddedAssigneeRemoved2022OutputResults.csv",index=False)
os.chmod("../csvResults/dfFinalDatasetAssigneeAddedAssigneeRemoved2022OutputResults.csv",0o777)
