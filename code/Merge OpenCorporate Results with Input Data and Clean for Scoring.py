#!/usr/bin/env python
# coding: utf-8

# # Merge OpenCorporate Results with PatentsView Input Data

# In[1]:


import pandas as pd
import numpy as np
import time
import re
import glob
import os
import warnings
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

# joined_files=os.path.join("../csvResults/rawData","dfFinalDatasetAssignee*2023*.csv")

# A list of all joined files is returned
# joined_list=glob.glob(joined_files)

# Finally, the files are joined
# df=pd.concat(map(pd.read_csv, joined_list), ignore_index=True)

df=pd.read_csv("../csvResults/dfFinalDatasetAssigneeSample_dates_5yr_output_cleaned.csv")
df.dropna(subset=['name'],inplace=True)
df.sort_values(by=['ID'],inplace=True)
# df1=df.iloc[:,[0,1,2,3,5,6,11,20,21,22,31,32,34,42,43,44,47,48,53]].reset_index(drop=True).copy()
df1=df.iloc[:,[0,1,2,3,5,6,21,22,34,43,44,47,48]].reset_index(drop=True).copy()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df1.assignee_id.nunique(),"\n")

display(df1.info(null_counts=True),df1.head())


# In[ ]:





# In[2]:


### start timer
t0=time.time()

### set the path for the input file and save to variable
results_folder = "../csvResults/"
# input_file = "dfFinalDatasetAssigneeRemoved2022OutputResults.csv"
input_file = "dfFinalDatasetAssigneeAddedAssigneeRemoved2022OutputResults.csv"

input_directory=os.path.join(results_folder,input_file)
print(input_directory,"\n")

### imports the dfMergedFullDataSet file to merge onto Mike's output file using the ID column
openC=pd.read_csv(input_directory).iloc[:,[0,1,2,3,4,5,6,7,10,12,13,14,15,16]]
openC.ID=openC.ID.astype(int)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",openC.assignee.nunique())
print("The number of unique patents are:",openC.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(openC.info(null_counts=True),openC.head())


# In[ ]:





# In[3]:


### start timer
t0=time.time()

df1.drop(labels=['assignee_id'],axis=1,inplace=True)
mergeResults=openC.merge(df1,on=['ID'],how='inner')
mergeResults1=mergeResults.loc[mergeResults['name'].notnull()]

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",mergeResults1.assignee_id.nunique())
print("The number of unique patents are:",mergeResults1.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(mergeResults1.info(null_counts=True),mergeResults1.head())


# In[ ]:





# In[4]:


# mergeResults1.to_csv("../csvResults/mergedOCResultsAndPVInput.csv",index=False)


# In[ ]:





# # Standardize the Organization Names, Cities, and States from PatentsView and OpenCorporates

# In[1]:


import pandas as pd
import numpy as np
import time
import re
import string
import os
import warnings
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

### set the path for the input file and save to variable
results_folder = "../csvResults/"
input_file = "mergedOCResultsAndPVInput.csv"

input_directory=os.path.join(results_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
df=pd.read_csv(input_directory)
df['organization']=df['organization'].str.lstrip().str.rstrip().str.title()
df['city']=df['city'].str.lstrip().str.rstrip().str.title()
df['state']=df['state'].str.lstrip().str.rstrip().str.title()
df['name']=df['name'].str.lstrip().str.rstrip().str.title()
df['jurisdiction_code']=df['jurisdiction_code'].str.lstrip().str.rstrip().str.title()

df['alternative_names']=df['alternative_names'].str.lstrip().str.rstrip().str.title()
df['previous_names']=df['previous_names'].str.lstrip().str.rstrip().str.title()

df['data']=df['data'].str.lstrip().str.rstrip().str.title()

df['address_city']=df['address_city'].str.lstrip().str.rstrip().str.title()
df['address_state']=df['address_state'].str.lstrip().str.rstrip().str.title()
df['agent_city']=df['agent_city'].str.lstrip().str.rstrip().str.title()
df['agent_state']=df['agent_state'].str.lstrip().str.rstrip().str.title()


last = df.pop('match_num')

df.insert(24, 'match_num', last)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df.info(null_counts=True),df.head())


# In[ ]:





# In[2]:


### this section cleans the organization and name fields by standardizing the names to facilitate better merge
### results. Skipping this section will yield poor results in any subsequent merges.

### start timer
t0=time.time()

### the pattern variable is utilized to create a 'list' of possible suffixes that should be removed
### from the organization and name fields. This list was constructed by manually inspecting the names
### prior to cleaning and does not represent a comprehensive list
pattern = '|'.join(['Llc','L\.L\.C\.','Inc\.$','Inc$','Ltd','\(','\)','Plc','P\.L\.C\.','Pllc','P\.L\.L\.C\.',
                    'Lp\.$','Lp$','Llp$','LP','L\.P\.','LC','L\.C\.','Ag$','Gmbh','SA$','Kg','Pvt','Sa$','BV','Nv$',
                    'Ab$','Pty$','SPA$','S\.P\.A\.','Bv','B\.V\.','B\.v\.','@','\.',','])

### converts the first character in each word to Uppercase and remaining characters to lowercase in 
### the string, followed by removing any whitespace that may exist to the left and right of the strings

df['name']=df['name'].str.replace('\s+',' ')

### some strings must be replaced rather than removed because the resulting organization names would
### not make sense or match incorrectly. For example, Arjang & Co., which is the full name for the
### organization, would become Arjang and would match to multiple records via the merge instead of
### one. This was observed through multiple trials of cleaning the data
df['name']=df['name'].str.replace(' & ', ' And ')
df['name']=df['name'].str.replace('&', ' And ')
df['name']=df['name'].str.replace(' - |-', ' ')
df['name']=df['name'].str.replace('+', ' ')
df['name']=df['name'].str.replace(' (Co\.$|Co$)', ' Company')
df['name']=df['name'].str.replace(' (Corp\.|Corp) | (Corp\.$|Corp$) | (Corp\.$|Corp$)|[Cc]orporation', '')
df['name']=df['name'].str.replace('Mfg', 'Manufacturing')
df['name']=df['name'].str.replace('Incorporated|Usa|Incorportated', '')

### apply the pattern variable to the organization field and clean the resulting whitespace to the
### left and right of the strings
df['name']=df['name'].str.replace(pattern, '')
df['name']=df['name'].str.lstrip().str.rstrip()

### convert any remaining names that are not standardized
df['name']=df['name'].str.replace(' (Co\.$|Co$)', ' Company')
df['name']=df['name'].str.replace("\'S", "\'s")
df['name']=df['name'].str.replace("L L C|Lc|Llc| Usa|Umi$|\/| Inc | Limited$| Llc", "")
df['name']=df['name'].str.replace("Dba |DBA ", " ")
df['name']=df['name'].str.replace("'|\?", "")
df['name']=df['name'].str.replace(" Company$| USA$", "")
df['name']=df['name'].str.replace("\s+", " ")
df['name']=df['name'].str.rstrip().str.lstrip()

df['organization']=df['organization'].str.replace("L L C|Lc|Llc| Usa|\/| Inc |\?| Ltd$| Ltd | Inc$| Company$", "")
df['organization']=df['organization'].str.replace("\s+", " ")
df['organization']=df['organization'].str.rstrip().str.lstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

display(df.info(),df.head())


# In[ ]:





# In[3]:


### start timer
t0=time.time()

### the pattern variable is utilized to create a 'list' of possible suffixes that should be removed
### from the organization and name fields. This list was constructed by manually inspecting the names
### prior to cleaning and does not represent a comprehensive list
pattern = '|'.join(['Llc','L\.L\.C\.','Inc\.$','Inc$','Ltd','\(','\)','Plc','P\.L\.C\.','Pllc','P\.L\.L\.C\.',
                    'Lp\.$','Lp$','Llp$','LP','L\.P\.','LC','L\.C\.','Ag$','Gmbh','SA$','Kg','Pvt','Sa$','BV','Nv$',
                    'Ab$','Pty$','SPA$','S\.P\.A\.','Bv','B\.V\.','B\.v\.','@','\.',','])

### converts the first character in each word to Uppercase and remaining characters to lowercase in 
### the string, followed by removing any whitespace that may exist to the left and right of the strings

df['assignee']=df['assignee'].str.replace('\s+',' ')

### some strings must be replaced rather than removed because the resulting organization names would
### not make sense or match incorrectly. For example, Arjang & Co., which is the full name for the
### organization, would become Arjang and would match to multiple records via the merge instead of
### one. This was observed through multiple trials of cleaning the data
df['assignee']=df['assignee'].str.replace(' & ', ' And ')
df['assignee']=df['assignee'].str.replace('&', ' And ')
df['assignee']=df['assignee'].str.replace(' - |-', ' ')
df['assignee']=df['assignee'].str.replace('+', ' ')
df['assignee']=df['assignee'].str.replace(' (Co\.$|Co$)', ' Company')
df['assignee']=df['assignee'].str.replace(' (Corp\.|Corp) | (Corp\.$|Corp$) | (Corp\.$|Corp$)|[Cc]orporation', '')
df['assignee']=df['assignee'].str.replace('Mfg', 'Manufacturing')
df['assignee']=df['assignee'].str.replace('Incorporated|Usa|Incorportated', '')

### apply the pattern variable to the organization field and clean the resulting whitespace to the
### left and right of the strings
df['assignee']=df['assignee'].str.replace(pattern, '')
df['assignee']=df['assignee'].str.lstrip().str.rstrip()

### convert any remaining names that are not standardized
df['assignee']=df['assignee'].str.replace(' (Co\.$|Co$)', ' Company')
df['assignee']=df['assignee'].str.replace("\'S", "\'s")
df['assignee']=df['assignee'].str.replace("L L C|Lc|Llc| Usa|Umi$|\/| Inc | Limited$| Llc", "")
df['assignee']=df['assignee'].str.replace("Dba |DBA ", " ")
df['assignee']=df['assignee'].str.replace("'|\?", "")
df['assignee']=df['assignee'].str.replace(" Company$| USA$", "")
df['assignee']=df['assignee'].str.replace("\s+", " ")
df['assignee']=df['assignee'].str.rstrip().str.lstrip()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

display(df.info(),df.head())


# In[ ]:





# In[4]:


### start timer
t0=time.time()
matches=[]

for i in range(len(df)):
    if df.iloc[i,3] == df.iloc[i,14]:
        matches.append(1)
    else:
        matches.append(0)      

df['matches']=matches        

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

display(df.info(),df.head())


# In[ ]:





# In[5]:


### start timer
t0=time.time()

### converts the first character in each word to Uppercase and remaining characters to Lowercase in 
### the string

### regex and regex1 were applied to initially find a list of organization names, followed by refining
### the names by removing erroneous characters
regex="'Company_Name': '[A-Z].+?'"
regex1="'[A-Z].+': "

### determines the length of the input data and creates 2 empty lists to be filled with the extracted
### organization names
b=len(df)
sub_finalAlt = []
finalAlt = []

### the for loop is initiated to review each record individually to extract the organization names
### under the alternative_names column
for j in range(b):
    
    ### if the record is empty or nan the record will be skipped
    if pd.isna(df.iloc[j,17]) is True:
        pass
    
    elif df.iloc[j,25] == 1:
        pass
    
    ### non-empty records have the curly brackets removed from the right and left of the list of
    ### strings. The findall function searches the list for all instances that match the regex
    ### variable and saves the list to the match variable. The length of this list is determined
    ### and provide to the nested loop below
    else:
        a=df.iloc[j,17][2:-2]
        match = re.findall(regex, a)
        c=len(match)
        
        ### for any length of c greater than 0, this for loop will evaluate each instance for the
        ### organization name by using the regex1 variable to remove characters that are not
        ### required. Once a match is found, the first character in each string is converted to
        ### an uppercase and saved to the sub_finalAlt list. The for loop repeats as many times
        ### that is equal to c
        for i in range(c):
            match1 = re.sub(regex1,"",match[i])
            match1=match1.replace("'","").title()
            
            ### ensures all spaces between strings are a single space
            match1 = re.sub('\s+',' ',match1)
            
            ### replaces specific characters
            match1=re.sub(' & ', ' And ',match1)
            match1=match1.replace('&', ' And ')
            match1=re.sub('Mfg', 'Manufacturing',match1)

            ### convert any names that are not standardized
            match1=re.sub(', Inc\.| Usa, Incorporated| Usa| Inc\.| Inc$| Incorpor$| Incorporated$|Incorporated', '',match1)
            match1=re.sub(', Llc$| Ltd\.| Ltd| Limited| Pty\.| Pty|L\.L\.C\.| Llp$| Llc$|\(.+?\)|,Inc\.', '',match1)
            match1=re.sub(', P\.C\.| P\.C\.| P\. C\.| D\.M\.D\.|D\.D\.S\.| D\. D\. S\.| M\.D\.|L\.L\.C', '',match1)
            match1=re.sub(' Corporation$| Corporation,$| Corp\.$ | Corp.$| Corp\.$| Corp\.,$', ' Corporation',match1)
            match1=re.sub('Co\.$| Co$', 'Company',match1)
            
            match1=re.sub("'", '',match1)
            match1=re.sub('L L C|Lc|Llc', '',match1)
            
            ### removes all punctuation and ensures all spaces were single spaced after processing
            ### the strings
            match1 = re.sub('-|/',' ',match1)
            match1 = match1.translate(str.maketrans('','',string.punctuation))
            match1 = re.sub(' $| LP$','',match1)
            match1 = re.sub('\s+',' ',match1)
                
            ### appends the strings to list
            sub_finalAlt.append(match1)
    
    ### after the nested for loop is finished extracting all possible organization names, the
    ### sub_finalAlt list is appended to the finalAlt list. The finalAlt list is updated with 
    ### the alternative names for each record after the for loops are finished and resets the
    ### sub_finalAlt to an empty list
    finalAlt.append(sub_finalAlt)
    sub_finalAlt = []

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

### prints the finalAlt list for review
print(finalAlt)


# In[ ]:





# In[6]:


### start timer
t0=time.time()

### creates 2 empty lists to be filled with the extracted organization names
sub_finalPre = []
finalPre = []

regex="'Company_Name': '[A-Za-z].+?'"
regex1="'[A-Za-z].+': "

### the for loop is initiated to review each record individually to extract the organization names
### under the previous_names column
for j in range(b):
    
    ### if the record is empty or nan the record will be skipped
    if pd.isna(df.iloc[j,18]) is True:
        pass
    
    elif df.iloc[j,25] == 1:
        pass
    
    ### non-empty records have the curly brackets removed from the right and left of the list of
    ### strings. The findall function searches the list for all instances that match the regex
    ### variable and saves the list to the match variable. The length of this list is determined
    ### and provide to the nested loop below
    else:
        a=df.iloc[j,18][2:-2]
        match = re.findall(regex, a)
        c=len(match)

        ### for any length of c greater than 0, this for loop will evaluate each instance for the
        ### organization name by using the regex1 variable to remove characters that are not
        ### required. Once a match is found, the first character in each string is converted to
        ### an uppercase and saved to the sub_finalPre list. The for loop repeats as many times
        ### that is equal to c
        for i in range(c):
            match1 = re.sub(regex1,"",match[i])
            match1=match1.replace("'","").title().lstrip().rstrip()
            
            ### ensures all spaces between strings are a single space
            match1 = re.sub('\s+',' ',match1)
            
            ### replaces specific characters
            match1=re.sub(' & ', ' And ',match1)
            match1=match1.replace('&', ' And ')
            match1=re.sub('Mfg', 'Manufacturing',match1)

            ### convert any names that are not standardized
            match1=re.sub(', Inc\.| Usa, Incorporated| Usa| Inc\.| Inc$| Incorpor$| Incorporated$|Incorporated', '',match1)
            match1=re.sub(', Llc$| Ltd\.| Ltd| Limited| Pty\.| Pty', '',match1)
            match1=re.sub('L\.L\.C\.| Llp$| Llc$|\(.+?\)|,Inc\.', '',match1)
            match1=re.sub(', P\.C\.| P\.C\.| P\. C\.| D\.M\.D\.|D\.D\.S\.| D\. D\. S\.| M\.D\.', '',match1)
            match1=re.sub(' Corporation$| Corporation,$| Corp\.$ | Corp.$| Corp\.$| Corp\.,$', ' Corporation',match1)
            match1=re.sub('Co\.$| Co$', ' Company',match1)
            
            match1=re.sub("'", '',match1)
            match1=re.sub('L L C|Lc|Llc', '',match1)
            
            ### removes all punctuation and ensures all spaces were single spaced after processing
            ### the strings
            match1 = re.sub('-|/',' ',match1)
            match1 = match1.translate(str.maketrans('','',string.punctuation))
            match1 = re.sub(' $| LP$','',match1)
            match1 = re.sub('\s+',' ',match1)
            
            sub_finalPre.append(match1)

    ### after the nested for loop is finished extracting all possible organization names, the
    ### sub_finalPre list is appended to the finalAlt list. The finalPre list is updated with 
    ### the previous names for each record after the for loops are finished and resets the
    ### sub_finalPre to an empty list
    finalPre.append(sub_finalPre)
    sub_finalPre = []

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

### prints the finalPre list for review
print(finalPre)


# In[ ]:





# In[7]:


### start timer
t0=time.time()

### append the OC_results dataset with the finalAlt and finalPre lists with a _clean suffix
df['alternative_names_clean'] = finalAlt
df['previous_names_clean'] = finalPre

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df.info(),df.head())


# In[ ]:





# In[8]:


### drop the alternative_names and previous_names columns and rearrange the alternative_names_clean 
### and previous_names_clean features
### start timer
t0=time.time()

df1=df.copy()
df1.drop(labels=['alternative_names','previous_names','matches'],axis=1,inplace=True)

thr_col = df1.pop('alternative_names_clean')
for_col = df1.pop('previous_names_clean')

df1.insert(15, 'alternative_names_clean', thr_col)
df1.insert(16, 'previous_names_clean', for_col)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df1.info(),df1.head())


# In[ ]:





# In[9]:


# df1.to_csv("../csvResults/mergedOCResultsAndPVInputClnNames.csv",index=False)


# In[ ]:





# # Standardize City and State Names

# In[1]:


import pandas as pd
import numpy as np
import time
import re
import string
import os
import warnings
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

### set the path for the input file and save to variable
results_folder = "../csvResults/"
input_file = "mergedOCResultsAndPVInputClnNames.csv"

input_directory=os.path.join(results_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
df=pd.read_csv(input_directory)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df.info(null_counts=True),df.head())


# In[ ]:





# In[2]:


### start timer
t0=time.time()

df['city']=df['city'].str.replace("St\. |^St ","Saint ")
df['city']=df['city'].str.replace("Ft\. |^Ft ","Fort ")
df['city']=df['city'].str.replace("Mt\. |^Mt ","Mount ")
df['city']=df['city'].str.replace("Pte\. |^Pte ","Pointe ")

df['address_city']=df['address_city'].str.replace("St\. |^St ","Saint ")
df['address_city']=df['address_city'].str.replace("Ft\. |^Ft ","Fort ")
df['address_city']=df['address_city'].str.replace("Mt\. |^Mt ","Mount ")
df['address_city']=df['address_city'].str.replace("Pte\. |^Pte ","Pointe ")
df['address_city']=df['address_city'].str.replace("Mpls","Minneapolis")
df['address_city']=df['address_city'].str.replace("^.+?, ","")

df['agent_city']=df['agent_city'].str.replace("St\. |^St ","Saint ")
df['agent_city']=df['agent_city'].str.replace("Ft\. |^Ft ","Fort ")
df['agent_city']=df['agent_city'].str.replace("Mt\. |^Mt ","Mount ")
df['agent_city']=df['agent_city'].str.replace("Pte\. |^Pte ","Pointe ")
df['agent_city']=df['agent_city'].str.replace("Mpls","Minneapolis")
df['agent_city']=df['agent_city'].str.replace("^.+?, ","")

df['data']=df['data'].str.replace("St\. |^St ","Saint ")
df['data']=df['data'].str.replace("Ft\. |^Ft ","Fort ")
df['data']=df['data'].str.replace("Mt\. |^Mt ","Mount ")
df['data']=df['data'].str.replace("Pte\. |^Pte ","Pointe ")

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

display(df.info(),df.head())


# In[ ]:





# In[3]:


### start timer
t0=time.time()

df['address_state']=df['address_state'].str.replace("^.+?, ","")
df['address_state']=df['address_state'].str.replace("^Pa [0-9].*","Pa")
df['address_state']=df['address_state'].str.replace("^.*Of ","")

df['agent_state']=df['agent_state'].str.replace("^.+?, ","")
df['agent_state']=df['agent_state'].str.replace("^Pa [0-9].*","Pa")
df['agent_state']=df['agent_state'].str.replace("^.*Of ","")

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

display(df.info(),df.head())


# In[ ]:





# In[4]:


### start timer
t0=time.time()
matchesC=[]
matchesS=[]

for i in range(len(df)):
    if df.iloc[i,4] == df.iloc[i,20]:
        matchesC.append(1)
    
    elif df.iloc[i,4] == df.iloc[i,22]:
        matchesC.append(1)
    
    else:
        matchesC.append(0)      

df['cityMatches']=matchesC   

for i in range(len(df)):
    if df.iloc[i,5] == df.iloc[i,21]:
        matchesS.append(1)
    
    elif df.iloc[i,5] == df.iloc[i,23]:
        matchesS.append(1)
    
    else:
        matchesS.append(0)      

df['stateMatches']=matchesS

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

display(df.info(),df.head())


# In[ ]:





# In[5]:


### start timer
t0=time.time()

### creates 2 empty lists to be filled with the extracted organization names
sub_finalCity = []
finalCity = []

regex="'Description': '[A-Za-z0-9].+?'"
regex1="'[A-Za-z].+': "

### the for loop is initiated to review each record individually to extract the organization names
### under the previous_names column
for j in range(len(df)):

    ### if the record is empty or nan the record will be skipped
    if pd.isna(df.iloc[j,19]) is True:
        pass
    
    elif df.iloc[j,25] == 1:
        pass
    
    ### non-empty records have the curly brackets removed from the right and left of the list of
    ### strings. The findall function searches the list for all instances that match the regex
    ### variable and saves the list to the match variable. The length of this list is determined
    ### and provide to the nested loop below
    else:
        a=df.iloc[j,19][2:-2]
        match = re.findall(regex, a)
        c=len(match)
        
        ### for any length of c greater than 0, this for loop will evaluate each instance for the
        ### organization name by using the regex1 variable to remove characters that are not
        ### required. Once a match is found, the first character in each string is converted to
        ### an uppercase and saved to the sub_finalPre list. The for loop repeats as many times
        ### that is equal to c
        for i in range(c):
            match1 = re.sub(regex1,"",match[i])
            match1 = match1.replace("'","")
            match1 = re.sub('\s+',' ',match1)
            match2=match1.split(",")

            for k in range(len(match2)):
                match2[k]=match2[k].lstrip().rstrip()

                if match2[k] == df.iloc[j,4] and len(sub_finalCity) == 0:
                    sub_finalCity.append(match2[k])

    ### after the nested for loop is finished extracting all possible organization names, the
    ### sub_finalPre list is appended to the finalAlt list. The finalPre list is updated with 
    ### the previous names for each record after the for loops are finished and resets the
    ### sub_finalPre to an empty list
    finalCity.append(sub_finalCity)
    sub_finalCity = []
    
### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")


# In[ ]:





# In[6]:


### start timer
t0=time.time()

### creates 2 empty lists to be filled with the extracted organization names
sub_finalState = []
finalState = []

regex="'Description': '[A-Za-z0-9].+?'"
regex1="'[A-Za-z].+': "

### the for loop is initiated to review each record individually to extract the organization names
### under the previous_names column
for j in range(len(df)):

    ### if the record is empty or nan the record will be skipped
    if pd.isna(df.iloc[j,19]) is True:
        pass
    
    elif df.iloc[j,25] == 1:
        pass
    
    ### non-empty records have the curly brackets removed from the right and left of the list of
    ### strings. The findall function searches the list for all instances that match the regex
    ### variable and saves the list to the match variable. The length of this list is determined
    ### and provide to the nested loop below
    else:
        a=df.iloc[j,19][2:-2]
        match = re.findall(regex, a)
        c=len(match)
        
        ### for any length of c greater than 0, this for loop will evaluate each instance for the
        ### organization name by using the regex1 variable to remove characters that are not
        ### required. Once a match is found, the first character in each string is converted to
        ### an uppercase and saved to the sub_finalPre list. The for loop repeats as many times
        ### that is equal to c
        for i in range(c):
            match1 = re.sub(regex1,"",match[i])
            match1 = match1.replace("'","")
            match1 = re.sub('\s+',' ',match1)
            match2=match1.split(",")

            for k in range(len(match2)):
                match2[k]=match2[k].lstrip().rstrip()
                    
                if match2[k] == df.iloc[j,5] and len(sub_finalState) == 0:
                    sub_finalState.append(match2[k])

    ### after the nested for loop is finished extracting all possible organization names, the
    ### sub_finalPre list is appended to the finalAlt list. The finalPre list is updated with 
    ### the previous names for each record after the for loops are finished and resets the
    ### sub_finalPre to an empty list

    finalState.append(sub_finalState)
    sub_finalState = []
    
### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")


# In[ ]:





# In[7]:


### start timer
t0=time.time()

### append the OC_results dataset with the finalAlt and finalPre lists with a _clean suffix
df['data_city'] = finalCity
df['data_state'] = finalState

df1=df.copy()
df1.drop(labels=['cityMatches','stateMatches'],axis=1,inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique patents are:",df.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df1.info(),df1.head())


# In[ ]:





# In[8]:


### start timer
t0=time.time()

sub_newCity=[]
newCity=[]
sub_newState=[]
newState=[]

for t in range(len(df1)):
    state=''.join(map(str,df1.data_state[t]))
    city=''.join(map(str,df1.data_city[t]))

    if df1.city[t] == df1.address_city[t]:
        sub_newCity.append(df1.address_city[t])
        pass
    
    elif df1.city[t] == df1.agent_city[t]:
        sub_newCity.append(df1.agent_city[t])
        pass
    
    elif df1.city[t] == city:
        sub_newCity.append(city)
        pass
    
    else:
        sub_newCity.append("")
    
    newCity.append(sub_newCity)
    sub_newCity=[]
           
    if df1.state[t] == df1.address_state[t]:
        sub_newState.append(df1.address_state[t])
        pass
    
    elif df1.state[t] == df1.agent_state[t]:
        sub_newState.append(df1.agent_state[t])
        pass
    
    elif df1.state[t] == state:
        sub_newState.append(state)
        pass
    
    else:
        sub_newState.append("")
        
    newState.append(sub_newState)
    sub_newState=[]

        
df1['cityMatch']=newCity
df1['stateMatch']=newState

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df1.assignee_id.nunique())
print("The number of unique patents are:",df1.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df1.info(),df1.head())


# In[ ]:





# In[9]:


### start timer
t0=time.time()

df2=df1.copy()
df2.drop(labels=['data','data_city','data_state'],axis=1,inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df2.assignee_id.nunique())
print("The number of unique patents are:",df2.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df2.info(),df2.head())


# In[ ]:





# In[10]:


### start timer
t0=time.time()

# df3=df2.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,15,16,25,26,20,21,22,23,13,18,19,14,17,24]].copy()
df3=df2.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,18,24,25,15,16,19,20,21,22,23]].copy()


### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df3.assignee_id.nunique())
print("The number of unique patents are:",df3.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df3.info(),df3.head())


# In[ ]:





# In[11]:


# df3.to_csv("../csvResults/mergedOCResultsAndPVInputClnNamesClnStCy.csv",index=False)


# In[ ]:





# ## Add Coordinates to all City Locations

# In[1]:


import pandas as pd
import numpy as np
import os
import time
import warnings
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

### set the path for the input file and save to variable
sour_folder="../sourceFiles/"
input_file = "location_suppLatLong1.tsv"

input_directory=os.path.join(sour_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
latLong=pd.read_csv(input_directory,sep="\t",usecols=['city','state','latitude','longitude'])

latLong.city=latLong.city.str.title()
latLong.state=latLong.state.str.title()
latLong.drop_duplicates(keep='first',inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")

### print general stats and first 5 records for dataset
display(latLong.info(null_counts=True),latLong.head())


# In[ ]:





# In[2]:


### start timer
t0=time.time()

### set the path for the input file and save to variable
res_folder = "../csvResults/"
input_file = "mergedOCResultsAndPVInputClnNamesClnStCy.csv"

input_directory=os.path.join(res_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
cleanCityStates=pd.read_csv(input_directory)
cleanCityStates.stateMatch=cleanCityStates.stateMatch.str.replace("\[|'|\]","")
cleanCityStates.cityMatch=cleanCityStates.cityMatch.str.replace("\[|'|\]","")

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",cleanCityStates.assignee_id.nunique())
print("The number of unique patents are:",cleanCityStates.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(cleanCityStates.info(null_counts=True),cleanCityStates.head())


# In[ ]:





# In[3]:


### start timer
t0=time.time()

#convert states from full names to two-letter abbreviations
cleanCityStates.replace({'address_state':{"California":"Ca","Connecticut":"Ct","Massachusetts":"Ma","Florida":"Fl",
                                          "Georgia":"Ga","Washington":"Wa","New York":"Ny","Delaware":"De","Tennessee":"Tn",
                                          "Missouri":"Mo","Texas":"Tx","Indiana":"In","Minnesota":"Mn","Pennsylvania":"Pa",
                                          "Oregon":"Or","Virginia":"Va","Illinois":"Il","Kentucky":"Ky","North Carolina":"Nc",
                                          "New Jersey":"Nj","Colorado":"Co","Maryland":"Md","Ohio":"Oh","Arizona":"Az",
                                          "Nevada":"Nv","Utah":"Ut","Michigan":"Mi","New Hampshire":"Nh","Vermont":"Vt",
                                          "Kansas":"Ks","Oklahoma":"Ok","Iowa":"Ia","Louisiana":"La","Rhode Island":"Ri",
                                          "Wisconsin":"Wi","Hawaii":"Hi","Montana":",Mt","Nebraska":"Ne",
                                          "District Of Columbia":"Dc","West Virginia":"Wv","Alabama":"Al","Idaho":"Id",
                                          "Maine":"Me","New Mexico":"Nm","South Carolina":"Sc","North Dakota":"Nd",
                                          "South Dakota":"Sd","Arkansas":"Ar","Alaska":"Ak","Wyoming":"Wy","Mississippi":"Ms"}},
                        inplace=True)

cleanCityStates.replace({'agent_state':{"California":"Ca","Connecticut":"Ct","Massachusetts":"Ma","Florida":"Fl",
                                          "Georgia":"Ga","Washington":"Wa","New York":"Ny","Delaware":"De","Tennessee":"Tn",
                                          "Missouri":"Mo","Texas":"Tx","Indiana":"In","Minnesota":"Mn","Pennsylvania":"Pa",
                                          "Oregon":"Or","Virginia":"Va","Illinois":"Il","Kentucky":"Ky","North Carolina":"Nc",
                                          "New Jersey":"Nj","Colorado":"Co","Maryland":"Md","Ohio":"Oh","Arizona":"Az",
                                          "Nevada":"Nv","Utah":"Ut","Michigan":"Mi","New Hampshire":"Nh","Vermont":"Vt",
                                          "Kansas":"Ks","Oklahoma":"Ok","Iowa":"Ia","Louisiana":"La","Rhode Island":"Ri",
                                          "Wisconsin":"Wi","Hawaii":"Hi","Montana":",Mt","Nebraska":"Ne",
                                          "District Of Columbia":"Dc","West Virginia":"Wv","Alabama":"Al","Idaho":"Id",
                                          "Maine":"Me","New Mexico":"Nm","South Carolina":"Sc","North Dakota":"Nd",
                                          "South Dakota":"Sd","Arkansas":"Ar","Alaska":"Ak","Wyoming":"Wy","Mississippi":"Ms"}},
                        inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",cleanCityStates.assignee_id.nunique())
print("The number of unique patents are:",cleanCityStates.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(cleanCityStates.info(null_counts=True),cleanCityStates.head())


# In[ ]:





# In[4]:


### start timer
t0=time.time()

for s in range(len(cleanCityStates)):
    if cleanCityStates.stateMatch[s] == "":
        cleanCityStates.stateMatch[s]=cleanCityStates.jurisdiction_code[s][3:]

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",cleanCityStates.assignee_id.nunique())
print("The number of unique patents are:",cleanCityStates.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(cleanCityStates.info(null_counts=True),cleanCityStates.head())


# In[ ]:





# In[5]:


### merge was performed ot join the lat/long coordinate data with the patentview/OC data

### start timer
t0=time.time()

matchLatLong=cleanCityStates.merge(latLong,left_on=['cityMatch','stateMatch'],right_on=['city','state'],
                                  how='outer',indicator=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",matchLatLong.assignee_id.nunique())
print("The number of unique patents are:",matchLatLong.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(matchLatLong.info(),matchLatLong.head())


# In[ ]:





# In[6]:


### start timer
t0=time.time()

matchLatLongBoth=matchLatLong.loc[matchLatLong['_merge']=='both']
matchLatLongBoth.rename(columns={'city_x':'city','state_x':'state','latitude_x':'latitude','longitude_x':'longitude',
                                 'latitude_y':'latitude_match','longitude_y':'longitude_match'},inplace=True)
matchLatLongBoth.drop(labels=['_merge','city_y','state_y'],axis=1,inplace=True)

matchLatLongLeft=matchLatLong.loc[matchLatLong['_merge']=='left_only']
matchLatLongLeft.rename(columns={'city_x':'city','state_x':'state','latitude_x':'latitude','longitude_x':'longitude',
                                'latitude_y':'latitude_match','longitude_y':'longitude_match'},inplace=True)
matchLatLongLeft.drop(labels=['_merge','city_y','state_y'],axis=1,inplace=True)

matchLatLongCon=pd.concat([matchLatLongBoth,matchLatLongLeft],axis=0)
matchLatLongCon1=matchLatLongCon.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,26,27,
                                         19,20,21,22,23,24,25]].copy()

matchLatLongCon1.drop_duplicates(keep='first',inplace=True)
matchLatLongCon2=matchLatLongCon1.sort_values(by=['ID']).reset_index(drop=True).copy()
matchLatLongCon2.ID=matchLatLongCon2.ID.astype(int)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",matchLatLongCon2.assignee_id.nunique())
print("The number of unique patents are:",matchLatLongCon2.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(matchLatLongCon2.info(),matchLatLongCon2.head())


# In[ ]:





# In[7]:


### merge was performed ot join the lat/long coordinate data with the patentview/OC data

### start timer
t0=time.time()

addLatLong=matchLatLongCon2.merge(latLong,left_on=['address_city','address_state'],right_on=['city','state'],
                                  how='outer',indicator=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",addLatLong.assignee_id.nunique())
print("The number of unique patents are:",addLatLong.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(addLatLong.info(),addLatLong.head())


# In[ ]:





# In[9]:


### start timer
t0=time.time()

addLatLongBoth=addLatLong.loc[addLatLong['_merge']=='both']
addLatLongBoth.rename(columns={'city_x':'city','state_x':'state','latitude_x':'latitude','longitude_x':'longitude',
                               'latitude_y':'latitude_add','longitude_y':'longitude_add'},inplace=True)
addLatLongBoth.drop(labels=['_merge','city_y','state_y'],axis=1,inplace=True)

addLatLongLeft=addLatLong.loc[addLatLong['_merge']=='left_only']
addLatLongLeft.rename(columns={'city_x':'city','state_x':'state','latitude_x':'latitude','longitude_x':'longitude',
                               'latitude_y':'latitude_add','longitude_y':'longitude_add'},inplace=True)
addLatLongLeft.drop(labels=['_merge','city_y','state_y'],axis=1,inplace=True)

addLatLongCon=pd.concat([addLatLongBoth,addLatLongLeft],axis=0)
addLatLongCon1=addLatLongCon.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
                                     21,22,23,24,28,29,25,26,27]].copy()

addLatLongCon1.drop_duplicates(keep='first',inplace=True)
addLatLongCon2=addLatLongCon1.sort_values(by=['ID']).reset_index(drop=True).copy()
addLatLongCon2.ID=addLatLongCon2.ID.astype(int)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",addLatLongCon2.assignee_id.nunique())
print("The number of unique patents are:",addLatLongCon2.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(addLatLongCon2.info(),addLatLongCon2.head())


# In[ ]:





# In[10]:


### merge was performed ot join the lat/long coordinate data with the patentview/OC data

### start timer
t0=time.time()

agtLatLong=addLatLongCon2.merge(latLong,left_on=['agent_city','agent_state'],right_on=['city','state'],
                                how='outer',indicator=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",agtLatLong.assignee_id.nunique())
print("The number of unique patents are:",agtLatLong.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(agtLatLong.info(),agtLatLong.head())


# In[ ]:





# In[11]:


### start timer
t0=time.time()

agtLatLongBoth=agtLatLong.loc[agtLatLong['_merge']=='both']
agtLatLongBoth.rename(columns={'city_x':'city','state_x':'state','latitude_x':'latitude','longitude_x':'longitude',
                               'latitude_y':'latitude_agt','longitude_y':'longitude_agt'},inplace=True)
agtLatLongBoth.drop(labels=['_merge','city_y','state_y'],axis=1,inplace=True)

agtLatLongLeft=agtLatLong.loc[agtLatLong['_merge']=='left_only']
agtLatLongLeft.rename(columns={'city_x':'city','state_x':'state','latitude_x':'latitude','longitude_x':'longitude',
                               'latitude_y':'latitude_agt','longitude_y':'longitude_agt'},inplace=True)
agtLatLongLeft.drop(labels=['_merge','city_y','state_y'],axis=1,inplace=True)

agtLatLongCon=pd.concat([agtLatLongBoth,agtLatLongLeft],axis=0)
agtLatLongCon1=agtLatLongCon.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,
                                     25,26,27,28,30,31,29]].copy()

agtLatLongCon1.drop_duplicates(keep='first',inplace=True)
agtLatLongCon2=agtLatLongCon1.sort_values(by=['ID']).reset_index(drop=True).copy()
agtLatLongCon2.ID=agtLatLongCon2.ID.astype(int)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",agtLatLongCon2.assignee_id.nunique())
print("The number of unique patents are:",agtLatLongCon2.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(agtLatLongCon2.info(),agtLatLongCon2.head())


# In[ ]:





# In[12]:


# agtLatLongCon2.to_csv("../csvResults/mergedOCResultsAndPVInputClnNamesClnStCyLongLat.csv",index=False)


# In[ ]:





# # Score the Organization Names for Patentsview against the OC Results

# In[1]:


import pandas as pd
import numpy as np
import os
import time
import warnings
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

### set the path for the input file and save to variable
res_folder = "../csvResults/"
input_file = "mergedOCResultsAndPVInputClnNamesClnStCyLongLat.csv"

input_directory=os.path.join(res_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
noOrgScores=pd.read_csv(input_directory)
noOrgScores.name=noOrgScores.name.str.title()

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noOrgScores.info(null_counts=True),noOrgScores.head())


# In[ ]:





# In[2]:


### start timer
t0=time.time()

noOrgScores['alternative_names_clean']=noOrgScores['alternative_names_clean'].str.replace("'","")
noOrgScores['previous_names_clean']=noOrgScores['previous_names_clean'].str.replace("'","")

e=[]
f=[]

for y in range(len(noOrgScores)):
    e.append(noOrgScores['alternative_names_clean'][y][1:-1].split(","))
    f.append(noOrgScores['previous_names_clean'][y][1:-1].split(","))
    
### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noOrgScores.info(null_counts=True),noOrgScores.head())


# In[ ]:





# In[3]:


### start timer
t0=time.time()

altPreList=[list(z) for z in zip(e, f)]
altPreList=pd.DataFrame(altPreList,columns=['new_alt','new_pre'])

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(altPreList.info(null_counts=True),altPreList.head())


# In[ ]:





# In[4]:


### start timer
t0=time.time()

noOrgScores['newAltClean'] = altPreList['new_alt']
noOrgScores['newPreClean'] = altPreList['new_pre']

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noOrgScores.info(),noOrgScores.head())


# In[ ]:





# In[5]:


### start timer
t0=time.time()

noOrgScores.drop(labels=['alternative_names_clean','previous_names_clean'],axis=1,inplace=True)
noOrgScores.rename(columns={'newAltClean':'alternative_names_clean', 'newPreClean':'previous_names_clean'},
                   inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noOrgScores.info(),noOrgScores.head())


# In[ ]:





# In[6]:


### score the organization names against the name, alternative_names_clean, and previous_names_clean features

t0=time.time()

noOrgScores['organization']=noOrgScores['organization'].str.lstrip()
noOrgScores['organization']=noOrgScores['organization'].str.rstrip()

a=len(noOrgScores)
mat1=[]
mat2=[]

for i in range(a):
    
    ### try/except is used to bypass cells with an NaN. Removing this will force the user
    ### to deal with exception errors
    try:

        ### calculate the fuzzy score for further evaluation
        q=fuzz.ratio(noOrgScores['organization'][i], noOrgScores['name'][i])
        ### most simplest and most representative in the data; if the score is 100, it is
        ### a perfect match, otherwise, the remaining code will resolve score differences
        if q == 100:
            mat1.append(q)
            mat2.append(noOrgScores['name'][i])

        elif ( q != 100 ):
            ### calculate the size of the data in each row for the alternative_names_clean
            ### column and previous_names_clean column
            r=len(noOrgScores['alternative_names_clean'][i])
            d=len(noOrgScores['previous_names_clean'][i])

            ### the remaining parts of the code uses if statements to step through the many
            ### conditions that may be present in the data. As each conditions is satisfied,
            ### the mat1 and mat2 lists are appended with the data; take note that q is the
            ### original score and s, v, e, and f are separate scores that are compared
            ### against q
            if r == 0 and d == 0:
                mat1.append(q)
                mat2.append(noOrgScores['name'][i])

            ### calculate the score for the alternative_names_clean and previous_names_clean
            ### columns. Once calculated, they are compared against each other and q to
            ### determine the score that is highest. The 'best' score is appened to mat1 and
            ### the name of the organization is appended to mat2. The same scorer is utilized 
            ### as the above but the process.extractOne function retrieves the organization
            ### match with the highest score and saves it as a tuple
            elif r == 1 and d == 1:
                s=process.extractOne(noOrgScores['organization'][i],
                                     noOrgScores['alternative_names_clean'][i],scorer=fuzz.ratio)
                e=process.extractOne(noOrgScores['organization'][i],
                                     noOrgScores['previous_names_clean'][i],scorer=fuzz.ratio)
                
                if s[1] > e[1] and s[1] > q:
                    mat1.append(s[1])
                    mat2.append(s[0])
                
                elif e[1] > s[1] and e[1] > q:
                    mat1.append(e[1])
                    mat2.append(e[0])

                else:
                    mat1.append(q)
                    mat2.append(noOrgScores['name'][i])


            ### this section is the same as above, except the code is looking at the
            ### alternative_names_clean column only
            elif r == 1 or d == 1:
                s=process.extractOne(noOrgScores['organization'][i],
                                     noOrgScores['alternative_names_clean'][i],scorer=fuzz.ratio)
                e=process.extractOne(noOrgScores['organization'][i],
                                     noOrgScores['previous_names_clean'][i],scorer=fuzz.ratio)

                if s[1] > q and s[1] > e[1]:
                    mat1.append(s[1])
                    mat2.append(s[0])
                
                elif e[1] > q and e[1] > s[1]:
                    mat1.append(e[1])
                    mat2.append(e[0])

                else:
                    mat1.append(q)
                    mat2.append(noOrgScores['name'][i])

            
            ### the following two sections resolve r and d lengths greater than 1 (i.e.,
            ### records that have more than 1 company names in the alternative_names_clean 
            ### and previous_names_clean columns)
            elif r > 1:
                v=process.extractOne(noOrgScores['organization'][i],
                                     noOrgScores['alternative_names_clean'][i],scorer=fuzz.ratio)                

                for p in range(r):

                    if v[1][p] > q:
                        mat1.append(v[1])
                        mat2.append(v[0])

                    elif v[1][p] == q:
                        mat1.append(q)
                        mat2.append(noOrgScores['name'][i])

                    else:
                        mat1.append(q)
                        mat2.append(noOrgScores['name'][i])
            
            elif d > 1:
                f=process.extractOne(noOrgScores['organization'][i], noOrgScores['previous_names_clean'][i],scorer=fuzz.ratio)

                for p in range(d):
                    
                    if f[1][p] > q:
                        mat1.append(f[1])
                        mat2.append(f[0])

                    elif f[1][p] == q:
                        mat1.append(q)
                        mat2.append(noOrgScores['name'][i])                    

                    else:
                        mat1.append(q)
                        mat2.append(noOrgScores['name'][i])

    except:
        ### error handling that places an NaN for every cells that does not have a value
        ### in the mat1 and/or mat2 lists
        mat1.append(np.nan)
        mat2.append(np.nan)
        
### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")


# In[ ]:





# In[7]:


### start timer
t0=time.time()

### concatenates the mat1 and mat2 lists into a dataframe
finalList=[list(w) for w in zip(mat1, mat2)]
finalListDf=pd.DataFrame(finalList,columns=['scores','names'])

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")

### print general stats and first 5 records for dataset
display(finalListDf.info(),finalListDf.head())


# In[ ]:





# In[8]:


### start timer
t0=time.time()

### appends the dataframe in which the organization names came from with the
### results from the scoring steps in the preceeding code blocks
noOrgScores['nameScores'] = finalListDf['scores']
noOrgScores['matchNames'] = finalListDf['names']

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noOrgScores.info(),noOrgScores.head())


# In[ ]:





# In[9]:


### start timer
t0=time.time()

noOrgScores1=noOrgScores.iloc[:,[0,1,2,3,4,5,6,7,8,9,11,12,13,32,33,15,16,17,18,19,20,
                                 21,22,23,24,25,26,27,28,29]].copy()
noOrgScores1.sort_values(by=['ID','nameScores'],ascending=[True,False],inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noOrgScores1.assignee_id.nunique())
print("The number of unique patents are:",noOrgScores1.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noOrgScores1.info(),noOrgScores1.head())


# In[ ]:





# In[10]:


# noOrgScores1.to_csv("../csvResults/mergedOCResultsAndPVInputClnNamesClnStCyLongLatOrgScore.csv",index=False)


# In[ ]:





# # Calculate Date and Location Differences

# In[1]:


import pandas as pd
import numpy as np
import os
import time
import warnings
from datetime import timedelta
from geopy.distance import geodesic
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

### set the path for the input file and save to variable
res_folder = "../csvResults/"
input_file = "mergedOCResultsAndPVInputClnNamesClnStCyLongLatOrgScore.csv"

input_directory=os.path.join(res_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
noDiff=pd.read_csv(input_directory)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noDiff.assignee_id.nunique())
print("The number of unique patents are:",noDiff.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noDiff.info(null_counts=True),noDiff.head())


# In[ ]:





# In[2]:


### start timer
# t0=time.time()

noDiff['incorporation_date'] = pd.to_datetime(noDiff['incorporation_date'],errors='coerce')
noDiff['dateFiledMin'] = pd.to_datetime(noDiff['dateFiledMin'],errors='coerce')
noDiff['record_date'] = pd.to_datetime(noDiff['record_date'],errors='coerce')

noDiff['dateDiff']=''

noDiff['date']=np.where(noDiff['dateFiledMin'] < noDiff['incorporation_date'],1,0)
noDiff['org']=np.where(noDiff['assignee'] == noDiff['organization'],1,0)

for i in range(len(noDiff)):
    if noDiff['date'][i] and noDiff['org'][i] == 1:
        try:
#             noDiff['dateDiff'][i]=round((noDiff['incorporation_date'][i]-noDiff['record_date'][i])/timedelta(days=365),3)
            noDiff['dateDiff'][i]=round((noDiff['record_date'][i]-noDiff['incorporation_date'][i])/timedelta(days=365),3)
            
        except:
            pass

    else:
#         noDiff['dateDiff'][i]=round((noDiff['incorporation_date'][i]-noDiff['dateFiledMin'][i])/timedelta(days=365),3)
        noDiff['dateDiff'][i]=round((noDiff['dateFiledMin'][i]-noDiff['incorporation_date'][i])/timedelta(days=365),3)

noDiff1=noDiff.iloc[:,0:31].sort_values(by=['ID']).copy()
noDiff1.drop(labels=['match_num'],axis=1,inplace=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noDiff1.assignee_id.nunique())
print("The number of unique patents are:",noDiff1.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noDiff1.info(),noDiff1.head())


# In[ ]:





# In[3]:


### start timer
t0=time.time()

### the distance between cities are calculated by utilizing the city_lat/city_long and the add_lat/add_long,
### agt_lat/agt_long, and data_lat/data_long features.
cityAddrCor=[]
cityAgtCor=[]
cityDataCor=[]

### each for loop below calculates the distance between the city found in PatentsView and the
### cities from the address_city, agent_city, and data_city. The try/except is included to deal
### with exception errors
for p in range(len(noDiff1)):
    
    try:
        cityCor = (noDiff1.iloc[p,6], noDiff1.iloc[p,7])
        cityMatchCor = (noDiff1.iloc[p,19], noDiff1.iloc[p,20])

        cityDataCor.append(geodesic(cityCor, cityMatchCor).miles)
    
    except:
        cityDataCor.append(np.nan)
        
for p in range(len(noDiff1)):
    
    try:
        cityCor = (noDiff1.iloc[p,6], noDiff1.iloc[p,7])
        cityAddressCor = (noDiff1.iloc[p,23], noDiff1.iloc[p,24])

        cityAddrCor.append(geodesic(cityCor, cityAddressCor).miles)
    
    except:
        cityAddrCor.append(np.nan)
        
for p in range(len(noDiff1)):
    
    try:
        cityCor = (noDiff1.iloc[p,6], noDiff1.iloc[p,7])
        cityAgentCor = (noDiff1.iloc[p,27], noDiff1.iloc[p,28])

        cityAgtCor.append(geodesic(cityCor, cityAgentCor).miles)
    
    except:
        cityAgtCor.append(np.nan)


        
### add the distance to the input dataframe and round the value
cityAddrCor1=[round(num, 1) for num in cityAddrCor]
cityAgtCor1=[round(num1, 1) for num1 in cityAgtCor]
cityDataCor1=[round(num1, 1) for num1 in cityDataCor]

noDiff1['cityToAddrDistance'] = cityAddrCor1
noDiff1['cityToAgtDistance'] = cityAgtCor1
noDiff1['cityToDataDistance'] = cityDataCor1

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",noDiff1.assignee_id.nunique())
print("The number of unique patents are:",noDiff1.patent.nunique(),"\n")

### print general stats and first 5 records for dataset
display(noDiff1.info(),noDiff1.head())


# In[ ]:





# In[4]:


# ### start timer
# t0=time.time()

# noDiff1=noDiff.iloc[:,[0,1,2,3,4,5,8,9,10,11,12,13,14,15,16,17,18,19,21,22,25,26,29,30,31,32,33]].reset_index(drop=True).copy()


# ### end timer and print total time
# t1=time.time()
# total=t1-t0
# print("Total time is %4f" % (total/60), "mins\n")
# print("The number of unique assignee IDs are:",noDiff1.assignee_id.nunique())
# print("The number of unique patents are:",noDiff1.patent.nunique(),"\n")

# ### print general stats and first 5 records for dataset
# display(noDiff1.info(),noDiff1.head())


# In[ ]:





# In[5]:


# noDiff1.to_csv("../csvResults/readyForScoring.csv",index=False)
# noDiff1.to_csv("../csvResults/readyForScoring2.csv",index=False)
# noDiff1.to_csv("../csvResults/readyForScoring5.csv",index=False)


# In[ ]:




