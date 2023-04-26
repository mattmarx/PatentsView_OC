#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import os
import time
import warnings
warnings.filterwarnings('ignore')

t0=time.time()

srcFiles="../sourceFiles/oldfiles"

# loads assignee dataset and filters for US company or corp (2)
assignee=pd.read_csv(os.path.join(srcFiles,"assignee.tsv"),sep='\t',usecols=['id','type','organization'])
subAssignee=assignee.loc[(assignee['type']==2)].reset_index(drop=True).iloc[:,[0,2]].copy()
subAssignee.organization=subAssignee.organization.str.title()
subAssignee.rename(columns={'id':'assignee_id'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(subAssignee.info(null_counts=True),subAssignee.head())


# In[ ]:





# In[17]:


t0=time.time()

pat_assignee=pd.read_csv(os.path.join(srcFiles,"patent_assignee.tsv"),sep='\t')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(pat_assignee.info(null_counts=True),pat_assignee.head())


# In[18]:


t0=time.time()

# loads location dataset for businesses and individuals
location=pd.read_csv(os.path.join(srcFiles,"location.tsv"),sep='\t',
                     usecols=['id','city','state','country','latitude','longitude'])
location.country=location.country.str.title()

location1=location.loc[location['country']=='Us'].iloc[:,[0,1,2,4,5]].copy()
location1.rename(columns={'id':'location_id'},inplace=True)
location2=location1[~location1['state'].str.contains("PR|NZ|NB|GU|GBN|TW|MH|SW|MX|MY|JP|AE|AP|VI|AV|NL|FI|MP|NS|UM|ON|                                                       CN|SCT|KR|FR|AA|QC|MJ|WS|VW|OA|CHA|LI|ML|KW",na=False,case=False)]
location3=location2[~location2['state'].str.contains("CN",na=False,case=False)]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(location3.info(null_counts=True),location3.head())


# In[ ]:





# In[19]:


t0=time.time()

# loads patent_id and patent_date - not really sure if this is important right now 1.31.2023
patentX=pd.read_csv(os.path.join(srcFiles,"patent.tsv"),sep='\t',usecols=['id','date'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(patentX.info(null_counts=True),patentX.head())


# In[20]:


t0=time.time()

# loads location dataset for businesses and individuals
patentX.rename(columns={'id':'patent','date':'dateGranted'},inplace=True)
patentX.dateGranted=pd.to_datetime(patentX.dateGranted,errors='coerce')
patentX.dateGranted=patentX.dateGranted.dt.strftime('%m/%d/%Y')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(patentX['patent'].unique()),"unique patent ids\n")
display(patentX.info(null_counts=True),patentX.head())


# In[ ]:





# In[21]:


import datetime

t0=time.time()

# loads patent_assignee dataset and is used as a xwalk between the assignee and location datasets
appDate=pd.read_csv(os.path.join(srcFiles,"application.tsv"),sep='\t',usecols=['patent_id','date'])

appDate.rename(columns={'patent_id':'patent','date':'dateFiled'},inplace=True)
appDate=appDate.dropna(subset=['dateFiled'])
appDate.dateFiled=pd.to_datetime(appDate.dateFiled,errors='coerce')
appDate.dateFiled=appDate.dateFiled.dt.strftime('%m/%d/%Y')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(appDate['patent'].unique()),"unique patent ids\n")
display(appDate.info(null_counts=True),appDate.head())


# In[ ]:





# In[22]:


t0=time.time()

# convert patent numbers from object to integer
patentX.patent=patentX.patent.astype(str)
appDate.patent=appDate.patent.astype(str)

# construct a dataset with date filed and date granted for each patent
patDates=appDate.merge(patentX,on='patent',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")

display(patDates.info(null_counts=True),patDates.head())


# In[ ]:





# In[23]:


t0=time.time()

patAssignee=pat_assignee.merge(subAssignee,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(patAssignee['patent_id'].unique()),"unique patent ids\n")
print("There are",len(patAssignee['assignee_id'].unique()),"unique assignees\n")
display(patAssignee.info(null_counts=True),patAssignee.head())


# In[ ]:





# In[24]:


t0=time.time()

patAssigneeLoc=patAssignee.merge(location3,on=['location_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(patAssigneeLoc['patent_id'].unique()),"unique patent ids\n")
print("There are",len(patAssigneeLoc['assignee_id'].unique()),"unique assignees\n")
display(patAssigneeLoc.info(null_counts=True),patAssigneeLoc.head())


# In[ ]:





# In[25]:


t0=time.time()

# convert patent numbers to object
patAssigneeLoc.patent_id=patAssigneeLoc.patent_id.astype(str)
patDates.patent=patDates.patent.astype(str)

# add the organization information to the patDates dataset
patDatesAssignee=patDates.merge(patAssigneeLoc,left_on='patent',right_on='patent_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(patDatesAssignee['patent'].unique()),"unique patent ids\n")
print("There are",len(patDatesAssignee['assignee_id'].unique()),"unique assignees\n")
display(patDatesAssignee.info(null_counts=True),patDatesAssignee.head())


# In[ ]:





# In[39]:


patDatesAssignee[patDatesAssignee['organization'].str.contains('Cora Aero',na=False,case=False)]
# patDatesAssignee[patDatesAssignee['assignee_id'].str.contains('003060db-56e8-43fe-9624-3e565ed23f89',na=False,case=False)]


# In[ ]:





# In[11]:


t0=time.time()

# add the organization information to the patDates dataset
dfPreFinal=patDatesAssignee.iloc[:,[4,5,6,7,8,9,10,0,1,2]].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(dfPreFinal.info(null_counts=True),dfPreFinal.head())


# In[ ]:





# ## Extract Minimum Dates for Filed and Granted

# In[12]:


t0=time.time()

dfPreFinal.dateFiled=pd.to_datetime(dfPreFinal.dateFiled,errors='coerce')
# dfPreFinal.dateFiled=dfPreFinal.dateFiled.dt.strftime('%m/%d/%Y')

dfPreFinal.dateGranted=pd.to_datetime(dfPreFinal.dateGranted,errors='coerce')
# dfPreFinal.dateGranted=dfPreFinal.dateGranted.dt.strftime('%m/%d/%Y')

dfDate=dfPreFinal.sort_values(['patent','assignee_id'],ascending=False).groupby(['assignee_id'],as_index=False)['dateFiled'].min()
dgDate=dfPreFinal.sort_values(['patent','assignee_id'],ascending=False).groupby(['assignee_id'],as_index=False)['dateGranted'].min()

dfDate.rename(columns={'dateFiled':'dateFiledMin'},inplace=True)
dgDate.rename(columns={'dateGranted':'dateGrantedMin'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(dfDate.info(null_counts=True),dgDate.info(null_counts=True))


# In[13]:


t0=time.time()

minDates=dfDate.merge(dgDate,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(minDates.info(null_counts=True),minDates.head())


# In[14]:


t0=time.time()

dfPreFinalMinDates=dfPreFinal.merge(minDates,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(dfPreFinalMinDates['patent'].unique()),"unique patent ids\n")
print("There are",len(dfPreFinalMinDates['assignee_id'].unique()),"unique assignees\n")
display(dfPreFinalMinDates.info(null_counts=True),dfPreFinalMinDates.head())


# In[ ]:





# ## Groupby Assignee Only

# In[15]:


t0=time.time()

patList=dfPreFinalMinDates.groupby('assignee_id',as_index=False)['patent'].agg(['unique']).reset_index(drop=False)
patList.rename(columns={'unique':'patentList'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(patList.info(null_counts=True),patList.head())


# In[ ]:





# In[16]:


t0=time.time()

dfFinal=dfPreFinalMinDates.merge(patList,on='assignee_id',how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(dfFinal['patent'].unique()),"unique patent ids\n")
print("There are",len(dfFinal['assignee_id'].unique()),"unique assignees\n")
display(dfFinal.info(null_counts=True),dfFinal.head())


# In[ ]:





# In[17]:


t0=time.time()

dfFinal1=dfFinal.sort_values(['assignee_id','dateFiled'],ascending=[False,True])

dfFinal1.drop_duplicates(subset=['assignee_id'],keep='first',inplace=True)
dfFinal2=dfFinal1.iloc[:,[0,1,2,3,4,5,6,8,9,10,11,12]].sort_values('assignee_id').reset_index(drop=True).copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(dfFinal2['assignee_id'].unique()),"unique assignees\n")
display(dfFinal2.info(null_counts=True),dfFinal2.head())


# In[18]:


# dfFinal2.to_csv("../csvResults/pvDatasetPubCompanyAssignee2022PVData.csv",index=False)


# In[ ]:





# ## Process Data

# In[1]:


import pandas as pd
import os
import datetime
import time
import warnings
warnings.filterwarnings('ignore')

t0=time.time()

saveFiles="../csvResults"

pvDataPubComp=pd.read_csv(os.path.join(saveFiles,"pvDatasetPubCompanyAssignee2022PVData.csv"))

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(pvDataPubComp['assignee_id'].unique()),"unique assignees\n")
display(pvDataPubComp.info(null_counts=True),pvDataPubComp.head())


# In[ ]:





# In[2]:


t0=time.time()

# removes universities and hospitals; list came from Matt
dfPreFinalMinDatesNoUniv=pvDataPubComp[pvDataPubComp['organization'].str.contains('Barts|Berkeley|Coll|Dept|Ecole|Hopkins|Hsch|                                 Max Planck|Med Sch|Politec|Polytec|School|Suny|Ucl|Umc|Univ|Institute|university|University|                                 faculty|Mit|Caltech|Nyu|Ucl|Univ|College|Ucsd|Cmu|Ucbl|Epfl|Lmu|Interuniv|Harvard|Case|                                 Western|Caltech|Cern|Chalmers|Cuny|Mit|Nyu|Riken|Th Darmstadt|Uc Davis|Ucsf|Usc|                                 Virginia Tech|Georgia Tech|Trustees Of',case=False)==False]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(dfPreFinalMinDatesNoUniv['assignee_id'].unique()),"unique assignees\n")
display(dfPreFinalMinDatesNoUniv.info(null_counts=True),dfPreFinalMinDatesNoUniv.head())


# In[ ]:





# In[3]:


t0=time.time()

srcFiles="../sourceFiles"

disNames=pd.read_stata(os.path.join(srcFiles,"patent_match_id_name.dta"),columns=['name_std'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")

display(disNames.info(null_counts=True),disNames.head())


# In[4]:


### this section cleans the organization and name fields by standardizing the names to facilitate better merge
### results. Skipping this section will yield poor results in any subsequent merges.

### start timer
t0=time.time()

### converts the first character in each word to Uppercase and remaining characters to lowercase in 
### the string, followed by removing any whitespace that may exist to the left and right of the strings
disNames['name_std']=disNames['name_std'].str.title()
disNames['name_std']=disNames['name_std'].str.lstrip().str.rstrip()
disNames['name_std']=disNames['name_std'].str.replace('\s+',' ')

### some strings must be replaced rather than removed because the resulting organization names would
### not make sense or match incorrectly. For example, Arjang & Co., which is the full name for the
### organization, would become Arjang and would match to multiple records via the merge instead of
### one. This was observed through multiple trials of cleaning the data
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

disNames['name_std']=disNames['name_std'].str.replace('Llc|Llc\.|Pllc|Pllc\.', '')


disNames['name_std']=disNames['name_std'].str.replace('\s+',' ')

### apply the pattern variable to the organization field and clean the resulting whitespace to the
### left and right of the strings
disNames['name_std']=disNames['name_std'].str.lstrip().str.rstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(disNames.info(),disNames.head())


# In[5]:


### this section cleans the organization and name fields by standardizing the names to facilitate better merge
### results. Skipping this section will yield poor results in any subsequent merges.

### start timer
t0=time.time()

### converts the first character in each word to Uppercase and remaining characters to lowercase in 
### the string, followed by removing any whitespace that may exist to the left and right of the strings
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.title()
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.lstrip().str.rstrip()
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('\s+',' ')

### some strings must be replaced rather than removed because the resulting organization names would
### not make sense or match incorrectly. For example, Arjang & Co., which is the full name for the
### organization, would become Arjang and would match to multiple records via the merge instead of
### one. This was observed through multiple trials of cleaning the data
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

dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('Llc|Llc\.|Pllc|Pllc\.', '')


dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.replace('\s+',' ')

### apply the pattern variable to the organization field and clean the resulting whitespace to the
### left and right of the strings
dfPreFinalMinDatesNoUniv['organization']=dfPreFinalMinDatesNoUniv['organization'].str.lstrip().str.rstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(dfPreFinalMinDatesNoUniv.info(),dfPreFinalMinDatesNoUniv.head())


# In[ ]:





# In[6]:


t0=time.time()

rmPubMerge=dfPreFinalMinDatesNoUniv.merge(disNames,left_on='organization',right_on='name_std',how='outer',indicator=True)
rmPubMerge1=rmPubMerge.loc[rmPubMerge['_merge']=='left_only'].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(rmPubMerge1['assignee_id'].unique()),"unique assignees\n")
display(rmPubMerge1.info(null_counts=True),rmPubMerge1.head())


# In[ ]:





# In[7]:


t0=time.time()

rmPubMerge2=rmPubMerge1.iloc[:,0:12].copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(rmPubMerge2['assignee_id'].unique()),"unique assignees\n")
display(rmPubMerge2.info(null_counts=True),rmPubMerge2.head())


# In[ ]:





# In[8]:


### start timer
t0=time.time()

## converts the first character in each word to Uppercase and remaining characters to lowercase in 
## the string, followed by removing any whitespace that may exist to the left and right of the strings
rmPubMerge2.organization=rmPubMerge2.organization.str.lstrip().str.rstrip()
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('\s+',' ')

### some strings must be replaced rather than removed because the resulting organization names would
### not make sense or match incorrectly. For example, Arjang & Co., which is the full name for the
### organization, would become Arjang and would match to multiple records via the merge instead of
### one. This was observed through multiple trials of cleaning the data
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('R&D', 'Research And Design')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('Medrea.+$', 'Medrea')

rmPubMerge2.organization=rmPubMerge2.organization.str.replace('Incorporated|Corporation|L\.P\.|A Division Of.+$| Company$', '')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace('Inc\.$| Company,$| Limited$| Limited,$|L\.L\.C\.| Lp$', '')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace(' Co\.,$|Corp\.,| Company,$| Lcc$| A Div\. Of.+$ Lc$', '')

rmPubMerge2.organization=rmPubMerge2.organization.str.replace(' Co\.,', ' Company')
rmPubMerge2.organization=rmPubMerge2.organization.str.replace("Int'L", 'International')

rmPubMerge2.organization=rmPubMerge2.organization.str.replace('\s+',' ')

rmPubMerge2=rmPubMerge2[~rmPubMerge2['organization'].str.contains('M\.D\.|Gmbh|Bv$|D/B/A/| Pte$| Pte | Trust$| Trustee ')]
rmPubMerge2=rmPubMerge2[~rmPubMerge2['organization'].str.contains(' b\.v\. ',case=False)]

### apply the pattern variable to the organization field and clean the resulting whitespace to the
### left and right of the strings
rmPubMerge2.organization=rmPubMerge2.organization.str.lstrip().str.rstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(rmPubMerge2['assignee_id'].unique()),"unique assignees\n")
display(rmPubMerge2.info(),rmPubMerge2.head())


# In[ ]:





# In[9]:


t0=time.time()

rmPubMerge2.organization=rmPubMerge2.organization.astype('str')
rmPubMerge2['rmPunc'] = rmPubMerge2.organization.str.replace('[^\w\s]','')
rmPubMerge2.rmPunc=rmPubMerge2.rmPunc.astype('str')

mask=rmPubMerge2.rmPunc.str.len() < 5
lessThan5=rmPubMerge2.loc[mask]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(lessThan5['assignee_id'].unique()),"unique assignees\n")
display(lessThan5.info(null_counts=True),lessThan5.head())


# In[10]:


# lessThan5.to_csv("../csvResults/lessThan5charactersAssignee2022PVData.csv")


# In[ ]:





# In[11]:


t0=time.time()

rmPubMerge2.rmPunc=rmPubMerge2.rmPunc.astype('str')

mask1=rmPubMerge2.rmPunc.str.len() > 4
moreThan4=rmPubMerge2.loc[mask1]

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(moreThan4['assignee_id'].unique()),"unique assignees\n")
display(moreThan4.info(null_counts=True),moreThan4.head())


# In[ ]:





# In[12]:


import datetime

t0=time.time()

dfFinal=moreThan4.iloc[:,[0,1,12,3,4,5,6,7,8,9,10,11]].copy()
dfFinal.rename(columns={'rmPunc':'organization'},inplace=True)

dfFinal.dateFiled=pd.to_datetime(dfFinal.dateFiled,errors='coerce')
dfFinal.dateFiled=dfFinal.dateFiled.dt.strftime('%m/%d/%Y')

dfFinal.dateGranted=pd.to_datetime(dfFinal.dateGranted,errors='coerce')
dfFinal.dateGranted=dfFinal.dateGranted.dt.strftime('%m/%d/%Y')

dfFinal.dateFiledMin=pd.to_datetime(dfFinal.dateFiledMin,errors='coerce')
dfFinal.dateFiledMin=dfFinal.dateFiledMin.dt.strftime('%m/%d/%Y')

dfFinal.dateGrantedMin=pd.to_datetime(dfFinal.dateGrantedMin,errors='coerce')
dfFinal.dateGrantedMin=dfFinal.dateGrantedMin.dt.strftime('%m/%d/%Y')

dfFinal1=dfFinal.reset_index(drop=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

print("There are",len(dfFinal['assignee_id'].unique()),"unique assignees\n")
display(dfFinal1.info(null_counts=True),dfFinal1.head())


# In[14]:


# dfFinal1.to_csv("../csvResults/dfFinalDatasetAssignee2022PVData.csv")


# In[ ]:




