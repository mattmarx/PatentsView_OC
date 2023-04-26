import pandas as pd
import os
import time
import warnings
warnings.filterwarnings('ignore')


"""
Author: Joshua Chu
Date: April 23, 2023

Description: this script constructs the assignee data set that comprises assignee information
that will be used to account for date descripencies between the minimum patent date from the
input file and the incorporation date from OpenCorporates. Specifically, this data set will
be applied to records where the incorporation date is 'younger' than the minimum patent date
provided in the input file.

The cases where the patent is assigned to an assignee that is different from the date indicated
in the input file can be resolved using this assignment file (output file from this script). A
specific example is Mod Golf Technologies LLC. The minimum patent date in the input file is
01/05/2017 and the incorporation date indicated in OpenCorporates is 01/28/2021. The scoring
algorithm will score this poorly. However, looking at the patent number that corresponds to
the 01/05/2017 date (US-11027175-B2), we can see the patent was not assigned to Mod Golf
Technologies LLC until 01/19/2021, which is more reasonable than the 01/05/2017. This indicates
the filing application date is incorrect and the later date is the appropriate date to use
in this case. Therefore, to resolve these issues, the assignee data set was constructed and
integrated into the PatentsView-OpenCorporates pipeline.

"""

### the following sections import the required documents to create the assignment data
### set that will contain the assignee name, assignor name, record_date, conveyance type
### title for the patent, application file number, application file date, patent number,
### and patent number date. Lastly, the rf_id is found in all source data sets and was
### utilized to join the data sets.
t0=time.time()

srcFiles="../sourceFiles/"

df=pd.read_csv(os.path.join(srcFiles,"assignee.csv"),usecols=['rf_id','ee_name'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df.info(null_counts=True),df.head())



t0=time.time()

df1=pd.read_csv(os.path.join(srcFiles,"assignor.csv"),usecols=['rf_id','or_name'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df1.info(null_counts=True),df1.head())


### using the rf_id, the assignee.csv and assignor.csv files were merged
t0=time.time()

merge1=df.merge(df1,on=['rf_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge1.info(null_counts=True),merge1.head())



t0=time.time()

df2=pd.read_csv(os.path.join(srcFiles,"assignment.csv"),usecols=['rf_id','record_dt'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df2.info(null_counts=True),df2.head())


### using the rf_id, the assignment.csv file was merged against the assignee-assignor
### merged set
t0=time.time()

merge2=merge1.merge(df2,on=['rf_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge2.info(null_counts=True),merge2.head())



t0=time.time()

df3=pd.read_csv(os.path.join(srcFiles,"assignment_conveyance.csv"),usecols=['rf_id','convey_ty'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df3.info(null_counts=True),df3.head())


### using the rf_id, the assignment_conveyance.csv file was merged against the
### assignee-assignor-assignement merged set
t0=time.time()

merge3=merge2.merge(df3,on=['rf_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge3.info(null_counts=True),merge3.head())



t0=time.time()

df4=pd.read_csv(os.path.join(srcFiles,"documentid.csv"),usecols=['rf_id','title','appno_doc_num','appno_date',
                                                                 'grant_doc_num','grant_date'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df4.info(null_counts=True),df4.head())


### using the rf_id, the documentid.csv file was merged against the assignee-assignor-
### assignement-assignment_convey merged set

from datetime import timedelta

t0=time.time()

merge4=merge3.merge(df4,on=['rf_id'],how='inner')
merge4.rename(columns={'ee_name':'assignee','or_name':'assignor','exec_dt':'exec_date','record_dt':'record_date',
                        'convey_ty':'convey_type','appno_doc_num':'app_file_num','appno_date':'app_file_date',
                        'grant_doc_num':'grant_num','grant_date':'grant_num_date'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge4.info(null_counts=True),merge4.head())


### using the merge4 data set the fields containing dates are converted to a different
### data type for sorting purposes
t0=time.time()

merge4['record_date'] = pd.to_datetime(merge4['record_date'],errors='coerce')
merge4['app_file_date'] = pd.to_datetime(merge4['app_file_date'],errors='coerce')
merge4['grant_num_date'] = pd.to_datetime(merge4['grant_num_date'],errors='coerce')

merge4.sort_values(by=['rf_id','record_date'],inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")
print("There are",merge4.rf_id.nunique(),"unique rf_ids\n")

display(merge4.info(null_counts=True),merge4.head())


### after sorting the data set, the convey_type field was utilized to filter for specific
### records containing the 'types' shown below. Following this step, any record that contains
### NAs for the grant_num and grant_num_date were dropped from the final data set. Finally,
### any rf_ids containing the same patent number and date were considered duplicates and
### dropped from the final data set.
t0=time.time()

filterMerge4=merge4.loc[(merge4.convey_type=='assignment') | (merge4.convey_type=='correct') |
                        (merge4.convey_type=='namechg') | (merge4.convey_type=='merger')].copy()
filterMerge4.dropna(subset=['grant_num','grant_num_date'],inplace=True)

filterMerge5=filterMerge4.drop_duplicates(subset=['rf_id','grant_num','grant_num_date'],keep='first').copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")
print("There are",filterMerge5.rf_id.nunique(),"unique rf_ids\n")

display(filterMerge5.info(null_counts=True),filterMerge5.head())


### save the file as reassignment.csv
filterMerge5.to_csv("../csvResults/reassignment.csv",index=False)
