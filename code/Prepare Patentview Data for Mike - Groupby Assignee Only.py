import pandas as pd
import os
import time
import datetime
import warnings
warnings.filterwarnings('ignore')

"""
Author: Joshua Chu
Date: April 23, 2023

Description: this script is the primary code that constructs the data set that will be sent
to Mike for pushing it through the OC API. In contrast to previous input files given to Mike,
this script now appends the assignee data that includes the assignee, assignor, and the date
the patent was assigned to a specific assignee. Throughout this script, strings representing
cities, states, and organization names were standardized to facilitate merges and subsequent
matching with the OC output provided by Mike. Further, a list of hospitals and organizations
that should not be included in the final data set was removed utilizing a list provided by
Matt. Lastly, in order to match IDs from this data to IDs already provided to Mike, the final
output file from this script will the input file for another Python script called:

  Use New dfFinalDatasetAssignee to Add Assignee Data to dfFinalDatasetAssigneeRemoved2022OutputResults.csv file.py

Failure to execute the script above will result in all subsequent scripts to fail.

"""

### the assignee.tsv file was imported and fields selected, then records
### were filtered using the assignee_type set to 2. The type 2 option
### keeps only US-based company.
t0=time.time()

srcFiles="../sourceFiles"

# loads assignee dataset and filters for US company or corp (2)
assignee=pd.read_csv(os.path.join(srcFiles,"assignee.tsv"),sep='\t',
                     usecols=['patent_id','assignee_id','disambig_assignee_organization','assignee_type','location_id'])
subAssignee=assignee.loc[(assignee['assignee_type']==2)].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(subAssignee['assignee_id'].unique()))
print("The total number of unique patents are:",len(subAssignee['patent_id'].unique()),"\n")

print(subAssignee.info(null_counts=True),flush=True)
print(subAssignee.head(),flush=True)


### steps below rename columns and drops records with no org names and
### locations
t0=time.time()

# rename columns
subAssignee.rename(columns={'patent_id':'patent','disambig_assignee_organization':'organization',
                    'assignee_type':'type','location_id':'location'},inplace=True)

subAssignee=subAssignee.dropna(subset=['organization','location'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(subAssignee['assignee_id'].unique()))
print("The total number of unique patents are:",len(subAssignee['patent'].unique()),"\n")

print(subAssignee.info(null_counts=True),flush=True)
print(subAssignee.head(),flush=True)


### loads location dataset for businesses and individuals and
### selects the required features
t0=time.time()

location=pd.read_csv(os.path.join(srcFiles,"location.tsv"),sep='\t',
                    usecols=['location_id','disambig_city','disambig_state','disambig_country','latitude','longitude'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print(location.info(null_counts=True),flush=True)
print(location.head(),flush=True)


### renames the features and further filters for US_based company. This
### is a necessary step because some foreign orgs were labeled with
### assignee_type of 2
t0=time.time()

location.rename(columns={'location_id':'location','disambig_city':'city','disambig_state':'state',
                         'disambig_country':'country'},inplace=True)

subLocation=location.loc[location['country']=='US'].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("There are",len(subLocation['location'].unique()),"unique locations\n")

print(subLocation.info(null_counts=True),flush=True)
print(subLocation.head(),flush=True)


### loads data from patent.tsv file and selects the required field
t0=time.time()

patentX=pd.read_csv(os.path.join(srcFiles,"patent.tsv"),sep='\t',usecols=['patent_id','patent_date'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique patents are:",len(patentX['patent_id'].unique()),"\n")

print(patentX.info(null_counts=True),flush=True)
print(patentX.head(),flush=True)


### renames the fields and converts date format
t0=time.time()

patentX.rename(columns={'patent_id':'patent','patent_date':'dateGranted'},inplace=True)
patentX.dateGranted=pd.to_datetime(patentX.dateGranted,errors='coerce')
patentX.dateGranted=patentX.dateGranted.dt.strftime('%m/%d/%Y')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique patents are:",len(patentX['patent'].unique()),"\n")

print(patentX.info(null_counts=True),flush=True)
print(patentX.head(),flush=True)


### loads application.tsv file and selects required fields, followed
### by renaming columns, converting date format, and dropping records
### with NAs in the dateField column
t0=time.time()

appDate=pd.read_csv(os.path.join(srcFiles,"application.tsv"),sep='\t',usecols=['patent_id','filing_date'])

appDate.rename(columns={'patent_id':'patent','filing_date':'dateFiled'},inplace=True)
appDate.dateFiled=pd.to_datetime(appDate.dateFiled,errors='coerce')
appDate.dateFiled=appDate.dateFiled.dt.strftime('%m/%d/%Y')
appDate=appDate.dropna(subset=['dateFiled'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique patents are:",len(appDate['patent'].unique()),"\n")

print(appDate.info(null_counts=True),flush=True)
print(appDate.head(),flush=True)


### converting data types to ensure the subsequent merge will not
### experience issues. An inner merge was performed using the patent
### number in both sets
t0=time.time()

patentX.patent=patentX.patent.astype(str)
appDate.patent=appDate.patent.astype(str)

patDates=appDate.merge(patentX,on='patent',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")
print("The total number of unique patents are:",len(patDates['patent'].unique()),"\n")

print(patDates.info(null_counts=True),flush=True)
print(patDates.head(),flush=True)


### converting data types to ensure the subsequent merge will not
### experience issues. An inner merge was performed using the patent
### number in both sets
t0=time.time()

subAssignee.patent=subAssignee.patent.astype(str)
patDates.patent=patDates.patent.astype(str)

patDatesAssignee=patDates.merge(subAssignee,on='patent',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(patDatesAssignee['assignee_id'].unique()))
print("The total number of unique patents are:",len(patDatesAssignee['patent'].unique()),"\n")

print(patDatesAssignee.info(null_counts=True),flush=True)
print(patDatesAssignee.head(),flush=True)


### converting data types to ensure the subsequent merge will not
### experience issues. An inner merge was performed using the location
### ids in both sets
t0=time.time()

patDatesAssignee.location=patDatesAssignee.location.astype(str)
subLocation.location=subLocation.location.astype(str)

patDatesAssigneeLoc=patDatesAssignee.merge(subLocation,on='location',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(patDatesAssigneeLoc['assignee_id'].unique()))
print("The total number of unique patents are:",len(patDatesAssigneeLoc['patent'].unique()),"\n")

print(patDatesAssigneeLoc.info(null_counts=True),flush=True)
print(patDatesAssigneeLoc.head(),flush=True)


### copy and select the required fields
t0=time.time()

dfPreFinal=patDatesAssigneeLoc.iloc[:,[3,6,4,7,8,10,11,0,1,2]].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfPreFinal['assignee_id'].unique()))
print("The total number of unique patents are:",len(dfPreFinal['patent'].unique()),"\n")

print(dfPreFinal.info(null_counts=True),flush=True)
print(dfPreFinal.head(),flush=True)


### the following extracts the minimum dates for application filed date
### and patent granted date. The initial code iteration utilized the
### patent granted date, but was recently switched to the application
### filed date. Both dates were kept in case the patent granted date
### could be utilized during the scoring steps. Records were grouped
### by patent and assigneed_id, then the date minimum was selected. The
### columns were renamed to correspond to the date
t0=time.time()

dfPreFinal.dateFiled=pd.to_datetime(dfPreFinal.dateFiled,errors='coerce')
dfPreFinal.dateGranted=pd.to_datetime(dfPreFinal.dateGranted,errors='coerce')

dfDate=dfPreFinal.sort_values(['patent','assignee_id'],
                              ascending=False).groupby(['assignee_id'],as_index=False)['dateFiled'].min()
dgDate=dfPreFinal.sort_values(['patent','assignee_id'],
                              ascending=False).groupby(['assignee_id'],as_index=False)['dateGranted'].min()

dfDate.rename(columns={'dateFiled':'dateFiledMin'},inplace=True)
dgDate.rename(columns={'dateGranted':'dateGrantedMin'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print(dfDate.info(null_counts=True),flush=True)
print(dgDate.info(null_counts=True),flush=True)


### the date granted data was merged against the date filed
### data set to obtain a complete record of dates for each
### patent, per assignee_id
t0=time.time()

minDates=dfDate.merge(dgDate,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print(minDates.info(null_counts=True),flush=True)
print(minDates.head(),flush=True)


### merged the data set containing the above dates to the main
### dataframe using an inner merge and merging on assignee_id
t0=time.time()

dfPreFinalMinDates=dfPreFinal.merge(minDates,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfPreFinalMinDates['assignee_id'].unique()))
print("The total number of unique patents are:",len(dfPreFinalMinDates['patent'].unique()),"\n")

print(dfPreFinalMinDates.info(null_counts=True),flush=True)
print(dfPreFinalMinDates.head(),flush=True)


### using assignee_ids the data set was grouped by assignee_id
### and a list patents per assignee was constructed and added to
### the end of the data set
t0=time.time()

patList=dfPreFinalMinDates.groupby('assignee_id',as_index=False)['patent'].agg(['unique']).reset_index(drop=False)
patList.rename(columns={'unique':'patentList'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(patList['assignee_id'].unique()),"\n")

print(patList.info(null_counts=True),flush=True)
print(patList.head(),flush=True)


### merged the patent list for each assignee to the main dataframe
### using the assignee_id
t0=time.time()

dfFinal=dfPreFinalMinDates.merge(patList,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfFinal['assignee_id'].unique()))
print("The total number of unique patents are:",len(dfFinal['patent'].unique()),"\n")

print(dfFinal.info(null_counts=True),flush=True)
print(dfFinal.head(),flush=True)


### the resulting data set was sorted using the fields below and
### using different ascending options. This prepares the data to
### have any duplicates dropped and keeping the first record. The
### resulting set will represent a list of unique assignees with
### the minimum date filed for the respective patent
t0=time.time()

dfFinal1=dfFinal.sort_values(['assignee_id','dateFiled','patent'],ascending=[False,True,False]).copy()

dfFinal1.drop_duplicates(subset=['assignee_id'],keep='first',inplace=True)
dfFinal2=dfFinal1.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12]].sort_values('assignee_id').reset_index(drop=True).copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfFinal2['assignee_id'].unique()))
print("The total number of unique patents are:",len(dfFinal2['patent'].unique()),"\n")

print(dfFinal2.info(null_counts=True),flush=True)
print(dfFinal2.head(),flush=True)

dfFinal2.to_csv("../csvResults/pvDatasetPubCompanyAssignee.csv",index=False)
os.chmod("../csvResults/pvDatasetPubCompanyAssignee.csv",0o777)

### append the assignment data that includes the assingee, assignor,
### record date, and patent number
t0=time.time()

srcFiles="../csvResults/"

ra=pd.read_csv(os.path.join(srcFiles,"reassignment.csv"),usecols=['assignee','assignor','record_date','grant_num'])
ra.rename(columns={'assignee':'assignee_id'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique patents are:",ra.grant_num.nunique(),"\n")

print(ra.info(null_counts=True),flush=True)
print(ra.head(),flush=True)


### import the primary data set that was constructed using the
### PatentsView data in earlier steps
t0=time.time()

pvDataPubComp=pd.read_csv(os.path.join(srcFiles,"pvDatasetPubCompanyAssignee.csv"))

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique patents are:",pvDataPubComp.patent.nunique())
print("The total number of unique assignee ids are:",len(pvDataPubComp['assignee_id'].unique()),"\n")

print(pvDataPubComp.info(null_counts=True),flush=True)
print(pvDataPubComp.head(),flush=True)


### perform an outer merge using the patent numbers and rename
### the columns. An outer merge is applied because we do not
### want to remove records that do not have corresponding
### assignee information.
t0=time.time()

assignMerge=pvDataPubComp.merge(ra,left_on=['patent'],right_on=['grant_num'],how='outer',indicator=True)
assignMerge.rename(columns={'assignee_id_x':'assignee_id','assignee_id_y':'assignee'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(assignMerge['assignee_id'].unique()))
print("The total number of unique patents are:",len(assignMerge['patent'].unique()),"\n")

print(assignMerge.info(null_counts=True),flush=True)
print(assignMerge.head(),flush=True)


### collect the data using the filters below, concatenate
### the data, and select the columns necessary for subsequent
### analyses. The assignee and assignor string were converted
### using the title() function
t0=time.time()

assignMergeboth=assignMerge[assignMerge['_merge']=='both']
assignMergeleft=assignMerge[assignMerge['_merge']=='left_only']

pvDataPubComp1=pd.concat([assignMergeboth,assignMergeleft],axis=0)
pvDataPubComp2=pvDataPubComp1.iloc[:,0:16].reset_index(drop=True).copy()

pvDataPubComp2.assignee=pvDataPubComp2.assignee.str.title()
pvDataPubComp2.assignor=pvDataPubComp2.assignor.str.title()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(pvDataPubComp2['assignee_id'].unique()))
print("The total number of unique patents are:",pvDataPubComp2.patent.nunique(),"\n")


print(pvDataPubComp2.info(null_counts=True),flush=True)
print(pvDataPubComp2.head(),flush=True)

pvDataPubComp2.to_csv("../csvResults/pvDatasetPubCompanyAssigneeAssignment.csv",index=False)
os.chmod("../csvResults/pvDatasetPubCompanyAssigneeAssignment.csv",0o777)

### the data set construction is finished, but the strings
### must be processed and specific orgs must be removed prior
### to using with the OC API
t0=time.time()

saveFiles="../csvResults"

pvDataPubComp=pd.read_csv(os.path.join(saveFiles,"pvDatasetPubCompanyAssigneeAssignment.csv"))

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(pvDataPubComp['assignee_id'].unique()))
print("The total number of unique patents are:",pvDataPubComp['patent'].nunique(),"\n")

print(pvDataPubComp.info(null_counts=True),flush=True)
print(pvDataPubComp.head(),flush=True)


### remove universities and hospitals using the list provided
### by Matt
t0=time.time()

dfPreFinalMinDatesNoUniv=pvDataPubComp[pvDataPubComp['organization'].str.contains('Barts|Berkeley|Coll|Dept|Ecole|Hopkins|Hsch|Max Planck|Med Sch|Politec|Polytec|School|Suny|Ucl|Umc|Univ|Institute|university|University|faculty|Mit|Caltech|Nyu|Ucl|Univ|College|Ucsd|Cmu|Ucbl|Epfl|Lmu|Interuniv|Harvard|Case|Western|Caltech|Cern|Chalmers|Cuny|Mit|Nyu|Riken|Th Darmstadt|Uc Davis|Ucsf|Usc|Virginia Tech|Georgia Tech|Trustees Of',case=False)==False]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfPreFinalMinDatesNoUniv['assignee_id'].unique()))
print("The total number of unique patents are:",dfPreFinalMinDatesNoUniv['patent'].nunique(),"\n")

print(dfPreFinalMinDatesNoUniv.info(null_counts=True),flush=True)
print(dfPreFinalMinDatesNoUniv.head(),flush=True)


### import another file Matt provided that will remove specific
### orgs from the data set. Since this data does not contain an
### assignee_id or any other unique identification, string matching
### using a merge must be performed. This requires the org names
### to be standardized
t0=time.time()

srcFiles="../sourceFiles"

disNames=pd.read_stata(os.path.join(srcFiles,"patent_match_id_name.dta"),columns=['name_std'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")

print(disNames.info(null_counts=True),flush=True)
print(disNames.head(),flush=True)


### this section cleans the organization and name fields by
### standardizing the names to facilitate better merge results.
### Skipping this section will yield poor results in any
### subsequent merges using this data
t0=time.time()

### converts the first character in each word to Uppercase and
### remaining characters to lowercase in the string, followed
### by removing any whitespace that may exist to the left and
### right of the strings
disNames['name_std']=disNames['name_std'].str.title()
disNames['name_std']=disNames['name_std'].str.lstrip().str.rstrip()
disNames['name_std']=disNames['name_std'].str.replace('\s+',' ')

### some strings must be replaced rather than removed because
### the resulting organization names would not make sense or
### match incorrectly. For example, Arjang & Co., which is the
### full name for the organization, would become Arjang and
### would match to multiple records via the merge instead of one.
### This was observed through multiple trials of cleaning the data
disNames['name_std']=disNames['name_std'].str.replace(' & ', ' And ')
disNames['name_std']=disNames['name_std'].str.replace(' (Co\.|Co)$', ' Company')
disNames['name_std']=disNames['name_std'].str.replace(' (Corp\.|Corp)$', ' Corporation')
disNames['name_std']=disNames['name_std'].str.replace('Intl', 'International')
disNames['name_std']=disNames['name_std'].str.replace('Mfg', 'Manufacturing')
disNames['name_std']=disNames['name_std'].str.replace(' (Inc\.|Inc)$| Inc ', ' Incorporated')
disNames['name_std']=disNames['name_std'].str.replace(' (Ltd\.|Ltd)$', ' Limited')
disNames['name_std']=disNames['name_std'].str.replace(' (Mgmt\.|Mgmt)$| (Mgmt\.|Mgmt)', ' Management')
disNames['name_std']=disNames['name_std'].str.replace('Solut$', 'Solutions')
disNames['name_std']=disNames['name_std'].str.replace('Gen Electr|Ge$', 'General Electric')
disNames['name_std']=disNames['name_std'].str.replace(' Gen ', ' General ')
disNames['name_std']=disNames['name_std'].str.replace('R C A', 'Rca')
disNames['name_std']=disNames['name_std'].str.replace('A M P', 'Amp')
disNames['name_std']=disNames['name_std'].str.replace('P P G', 'Ppg')
disNames['name_std']=disNames['name_std'].str.replace('G T E', 'Gte')
disNames['name_std']=disNames['name_std'].str.replace('G A F', 'Gaf')
disNames['name_std']=disNames['name_std'].str.replace('Ibm', 'Ibm Corporation')
disNames['name_std']=disNames['name_std'].str.replace('Sys |Syt$', 'Systems ')
disNames['name_std']=disNames['name_std'].str.replace('Ind ', 'Industries ')

### this line ensures the strings contain single spaces and
### strip the left and right of strings for extra whitespace
disNames['name_std']=disNames['name_std'].str.replace('\s+',' ')
disNames['name_std']=disNames['name_std'].str.lstrip().str.rstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print(disNames.info(),flush=True)
print(disNames.head(),flush=True)


### this section performs similar steps as above to standardize
### the organization names to facilitate better matching results.
### Skipping this step will result in a poor merge
t0=time.time()

### converts the first character in each word to Uppercase and
### remaining characters to lowercase in the string, followed
### by removing any whitespace that may exist to the left and
### right of the strings
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.title()
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.lstrip().str.rstrip()
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('\s+',' ')

### some strings must be replaced rather than removed because
### the resulting organization names would not make sense or
### match incorrectly. For example, Arjang & Co., which is the
### full name for the organization, would become Arjang and
### would match to multiple records via the merge instead of one.
### This was observed through multiple trials of cleaning the data
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' & ', ' And ')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' (Co\.|Co)$', ' Company')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' (Corp\.|Corp)$', ' Corporation')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Intl', 'International')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Mfg', 'Manufacturing')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' (Inc\.|Inc)$| Inc ',
                                                                                              ' Incorporated')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' (Ltd\.|Ltd)$', ' Limited')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' (Mgmt\.|Mgmt)$| (Mgmt\.|Mgmt)',
                                                                                              ' Management')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Solut$', 'Solutions')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Gen Electr|Ge$',
                                                                                              'General Electric')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace(' Gen ', ' General ')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('R C A', 'Rca')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('A M P', 'Amp')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('P P G', 'Ppg')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('G T E', 'Gte')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('G A F', 'Gaf')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Ibm', 'Ibm Corporation')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Sys |Syt$', 'Systems ')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Ind ', 'Industries ')

### this section ensures the strings contain single spaces and
### strip the left and right of strings for extra whitespace
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('\s+',' ')
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.lstrip().str.rstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfPreFinalMinDatesNoUniv['assignee_id'].unique()))
print("The total number of unique patents are:",dfPreFinalMinDatesNoUniv['patent'].nunique(),"\n")

print(dfPreFinalMinDatesNoUniv.info(),flush=True)
print(dfPreFinalMinDatesNoUniv.head(),flush=True)


### merge the two dataframes using the org names and outer merge.
### Any records that have the org names in both sets will be removed
### while those that remain will be kept
t0=time.time()

rmPubMerge=dfPreFinalMinDatesNoUniv.merge(disNames,left_on='organization',right_on='name_std',how='outer',indicator=True)
rmPubMerge1=rmPubMerge.loc[rmPubMerge['_merge']=='left_only'].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(rmPubMerge1['assignee_id'].unique()))
print("The total number of unique patents are:",rmPubMerge1['patent'].nunique(),"\n")

print(rmPubMerge1.info(null_counts=True),flush=True)
print(rmPubMerge1.head(),flush=True)


### selects and copies the necessary fields
t0=time.time()

rmPubMerge2=rmPubMerge1.iloc[:,0:16].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(rmPubMerge2['assignee_id'].unique()))
print("The total number of unique patents are:",rmPubMerge2['patent'].nunique(),"\n")

print(rmPubMerge2.info(null_counts=True),flush=True)
print(rmPubMerge2.head(),flush=True)


### with as many of the orgs removed as possible, the next steps
### standardize the organization names to remove suffixes with
### the resulting names used in the OC API. Many of the steps
### below are similar to the above steps, with the major exception
### being the removal of suffixes. There are some cases where
### organizations have foreign suffixes that were not removed in
### previous steps. Steps to remove those records were taken using
### the code below
t0=time.time()

rmPubMerge2.organization=rmPubMerge2.organization.str.lstrip().str.rstrip()
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('\s+',' ')

rmPubMerge2.organization=rmPubMerge2.organization.str.replace('R&D', 'Research And Design')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('Medrea.+$', 'Medrea')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('Incorporated|Corporation|L\.P\.|A Division Of.+$| Company$', '')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('Inc\.$| Company,$| Limited$| Limited,$|L\.L\.C\.| Lp$', '')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace(' Co\.,$|Corp\.,| Company,$| Lcc$| A Div\. Of.+$ Lc$', '')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace(' Llc$|,', '')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace(' Co\.,', ' Company')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace("Int'L", 'International')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('\s+',' ')

### this removes suspected foreign orgs
rmPubMerge2=rmPubMerge2[~rmPubMerge2['organization'].str.contains('M\.D\.|Gmbh|Bv$|D/B/A/| Pte$| Pte | Trust$| Trustee ')]
rmPubMerge2=rmPubMerge2[~rmPubMerge2['organization'].str.contains(' b\.v\. ',case=False)]

### this section ensures the strings contain single spaces and
### strip the left and right of strings for extra whitespace
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('\s+',' ')
rmPubMerge2.organization=rmPubMerge2.organization.str.lstrip().str.rstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(rmPubMerge2['assignee_id'].unique()))
print("The total number of unique patents are:",rmPubMerge2['patent'].nunique(),"\n")

print(rmPubMerge2.info(),flush=True)
print(rmPubMerge2.head(),flush=True)


### the section below removes orgs that have a string length
### of less than 5 characters long. In previous runs, the
### list of results from the OC API would be very long for
### these orgs and it was difficult to distinguish the correct
### OC match to the PatentsView data. Therefore, it was
### determined anything less than 5 characters long would be
### removed from this list and searched manually in the future.
t0=time.time()

rmPubMerge2.organization=rmPubMerge2.organization.astype('str')
rmPubMerge2['rmPunc'] = rmPubMerge2.organization.str.replace('[^\w\s]','')
rmPubMerge2.rmPunc=rmPubMerge2.rmPunc.astype('str')

mask=rmPubMerge2.rmPunc.str.len() < 5
lessThan5=rmPubMerge2.loc[mask]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(lessThan5['assignee_id'].unique()))
print("The total number of unique patents are:",lessThan5['patent'].nunique(),"\n")

print(lessThan5.info(null_counts=True),flush=True)
print(lessThan5.head(),flush=True)

lessThan5.to_csv("../csvResults/lessThan5charactersAssignee.csv")
os.chmod("../csvResults/lessThan5charactersAssignee.csv",0o777)

### the section below filters for records with organization
### names longer than 4 characters long
t0=time.time()

rmPubMerge2.rmPunc=rmPubMerge2.rmPunc.astype('str')

mask1=rmPubMerge2.rmPunc.str.len() > 4
moreThan4=rmPubMerge2.loc[mask1]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(moreThan4['assignee_id'].unique()))
print("The total number of unique patents are:",moreThan4['patent'].nunique(),"\n")

print(moreThan4.info(null_counts=True),flush=True)
print(moreThan4.head(),flush=True)


### the last section of code selects the fields, renames the
### necessary columns, and converts the dates to the format
### Mike requires to use with the OC API
t0=time.time()

dfFinal=moreThan4.iloc[:,0:16].copy()
dfFinal.rename(columns={'rmPunc':'organization'},inplace=True)

dfFinal.dateFiled=pd.to_datetime(dfFinal.dateFiled,errors='coerce')
dfFinal.dateFiled=dfFinal.dateFiled.dt.strftime('%m/%d/%Y')

dfFinal.dateGranted=pd.to_datetime(dfFinal.dateGranted,errors='coerce')
dfFinal.dateGranted=dfFinal.dateGranted.dt.strftime('%m/%d/%Y')

dfFinal.dateFiledMin=pd.to_datetime(dfFinal.dateFiledMin,errors='coerce')
dfFinal.dateFiledMin=dfFinal.dateFiledMin.dt.strftime('%m/%d/%Y')

dfFinal.dateGrantedMin=pd.to_datetime(dfFinal.dateGrantedMin,errors='coerce')
dfFinal.dateGrantedMin=dfFinal.dateGrantedMin.dt.strftime('%m/%d/%Y')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The total number of unique assignee ids are:",len(dfFinal['assignee_id'].unique()))
print("The total number of unique patents are:",dfFinal['patent'].nunique(),"\n")

print(dfFinal.info(null_counts=True),flush=True)
print(dfFinal.head(),flush=True)

# dfFinal.to_csv("../csvResults/dfFinalDatasetAssignee.csv")
dfFinal.to_csv("../csvResults/dfFinalDatasetAssigneeAddedAssignee.csv",index=False)
os.chmod("../csvResults/dfFinalDatasetAssigneeAddedAssignee.csv",0o777)
