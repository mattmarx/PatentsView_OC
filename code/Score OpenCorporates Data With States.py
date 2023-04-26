#!/usr/bin/env python
# coding: utf-8

# ## Scoring Algorithm

# In[1]:


import pandas as pd
import numpy as np
import time
import os
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')

### start timer
t0=time.time()

### set the path for the input file and save to variable
res_folder = "../csvResults/"
# input_file = "readyForScoring.csv"
# input_file = "readyForScoring2.csv"
input_file = "readyForScoring5.csv"

input_directory=os.path.join(res_folder,input_file)
print(input_directory,"\n")

### import the output file from OC API matching and select columns
df=pd.read_csv(input_directory)
df['incorporation_date'] = pd.to_datetime(df['incorporation_date'],errors='coerce')
df['dateFiledMin'] = pd.to_datetime(df['dateFiledMin'],errors='coerce')
df['record_date'] = pd.to_datetime(df['record_date'],errors='coerce')

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee_id.nunique())
print("The number of unique IDs are:",df.ID.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df.info(null_counts=True),df.head())


# In[ ]:





# In[2]:


### start timer
t0=time.time()

totalScore=[]
d=len(df)
x=''

### score the fuzzy match percentages

for s in range(d):
    ### scores all records with a fuzzy match score with a 100%; account for organization
    ### name lengths (i.e., shorter names are scored lower than longer names)
    
    if df['nameScores'][s] == 100:

        if len(df['organization'][s]) < 5:
            x=0
        
        elif len(df['organization'][s]) >= 5:
            x=5
    
    ### visual inspection of the data indicates a discrete group between 95% and 100%,
    ### resulting in the next set. Name lengths are again accounted for and shorter
    ### names/scores are downweighted even more
    
    elif 90 <= df['nameScores'][s] < 100:
        
        if len(df['organization'][s]) < 5:
            x=0
        
        elif 5 <= len(df['organization'][s]) < 10:
            x=1
            
        elif 10 <= len(df['organization'][s]) < 15:
            x=2
            
        elif len(df['organization'][s]) >= 15:
            x=5

    ### many of the fuzzy matches in this range are wrong, but there are a couple correct
    ### that should not be discounted. While the weights are not as high as the previous
    ### sections, correct matches will be given better scores than the next section
    
    elif 87 <= df['nameScores'][s] < 90:
        
        if len(df['organization'][s]) < 5:
            x=-1
        
        elif 5 <= len(df['organization'][s]) < 10:
            x=1
            
        elif 10 <= len(df['organization'][s]) < 15:
            x=2
            
        elif len(df['organization'][s]) >= 15:
            x=4

    ### very few are correct, but there are some misspellings that were not collected during
    ### the cleaning phase. Many of the records will be weighted down, but those that are
    ### longer in length will be given higher scores than names that are shorter
           
    elif df['nameScores'][s] < 87:
        x=-7
        
    elif np.isnan(df['nameScores'][s]):
        x=-10

    ### scoring the different features that contain state information for each
    ### organization. The jurisdictionScore feature was created by extracting
    ### the state from the jurisdiction_code field. The jurisdiction_code
    ### feature is the primary metric used to match patentsview and OC records
    ### and therefore, given a larger weight. The stateAddScore is given the
    ### second highest weight because it is the primary address that is listed
    ### in an OC record. stateAgtScore is given the least amount of weight
    ### because the agent may not always be located at the registered address 
    ### for the organization.
    
    if df['state'][s] == df['stateMatch'][s]:
        x=x+5
        
    elif df['state'][s] == df['address_state'][s]:
        x=x+5
        
    elif df['state'][s] == df['agent_state'][s]:
        x=x+5
        
    if df['city'][s] == df['cityMatch'][s]:
        x=x+5
        a=1
    
    elif df['city'][s] == df['address_city'][s]:
        x=x+5
        a=1
    
    elif df['city'][s] == df['agent_city'][s]:
        x=x+5
        a=1
        
    else:
        a=0
        
    ### the code below penalizes records with a first patent applied for date that is older
    ### than the incorporation date for that organization
    
    if df['dateFiledMin'][s] < df['incorporation_date'][s]:
        
        if df['dateDiff'][s] >= -5 and df['dateDiff'][s] < 0:
            x=x-1
            
        elif df['dateDiff'][s] >= -10 and df['dateDiff'][s] < -5:
            x=x-3
            
        elif df['dateDiff'][s] < -10:
            x=x-5
            
    ### the address_city feature is weighted more than the agent_city column for similar
    ### reasons stated in the states section above. Cities less than 4 characters long
    ### are penalized and gradually score better as the character length increase. Moreover,
    ### the score from fuzzy matching is used to create groups as shown below. Fuzzy
    ### scores below 90% are weighted negatively
    

    if a == 1:
        pass
    
    elif 0 <= df['cityToAddrDistance'][s] < 50 or 0 <= df['cityToAgtDistance'][s] < 50 or 0 <= df['cityToDataDistance'][s] < 50:
        x=x+5
        
    elif 50 <= df['cityToAddrDistance'][s] < 100 or 50 <= df['cityToAgtDistance'][s] < 100 or 50 <= df['cityToDataDistance'][s] < 100:
        x=x+2
        
    elif 100 <= df['cityToAddrDistance'][s] < 200 or 100 <= df['cityToAgtDistance'][s] < 200 or 100 <= df['cityToDataDistance'][s] < 200:
        x=x+1
    
    elif df['cityToAddrDistance'][s] >= 200 or df['cityToAgtDistance'][s] >= 200 or df['cityToDataDistance'][s] >= 200:
        x=x-2
    
    elif np.isnan(df['cityToDataDistance'][s]):
        x=x+0
         
    
    try:
        if df['dateDiff'][s] <= 15:
            x=x+5

        elif 15 < df['dateDiff'][s] <= 25:
            x=x+4
        
        elif 25 < df['dateDiff'][s] <= 30:
            x=x+3
            
        elif 30 < df['dateDiff'][s] <= 35:
            x=x+2
            
        elif df['dateDiff'][s] > 35:
            x=x+1

    except:
        print('cannot compare dates')
    
    totalScore.append(x)
    a=0
    
### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")


# In[ ]:





# In[3]:


### start timer
t0=time.time()

df['totalScore'] = totalScore

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee.nunique())
print("The number of unique IDs are:",df.ID.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df.info(),df.head())


# In[ ]:





# In[4]:


# df.to_csv("../csvResults/scoredOCResults5.csv",index=False)


# In[ ]:





# # Calculate Confidence Scores

# In[5]:


### start timer
t0=time.time()

df['confidenceScore']=((10-1)*((df['totalScore']-min(df['totalScore']))/
                               (max(df['totalScore'])-min(df['totalScore']))))+1

df['confidenceScore']=[round(num1, 2) for num1 in df['confidenceScore']]

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee.nunique())
print("The number of unique IDs are:",df.ID.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df.info(),df.head())


# In[ ]:





# In[6]:


### start timer
t0=time.time()

df['score'] = df['confidenceScore'].apply(np.floor)
df1=df.sort_values(by=['ID','score','dateDiff','record_date'],ascending=[True,False,False,False]).reset_index(drop=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")

### print general stats and first 5 records for dataset
display(df1.info(),df1.head())


# In[ ]:





# In[7]:


### start timer
t0=time.time()

df2=df1.drop_duplicates(subset=['ID'],keep='first')

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df2.assignee_id.nunique())
print("The number of unique IDs are:",df2.ID.nunique(),"\n")

### print general stats and first 5 records for dataset
display(df2.info(),df2.head())


# In[ ]:





# In[8]:


# df2.to_csv("../csvResults/reviewScoredResults5.csv",index=False)


# In[ ]:





# In[9]:


### +/- 5 yrs
# df2['score'].value_counts().sort_index(ascending=False)


# In[ ]:





# In[8]:


### +/- 5 yrs
# df2['score'].value_counts().sort_index(ascending=False)


# In[8]:


### +/- 2 yrs
# df2['score'].value_counts().sort_index(ascending=False)


# In[8]:


### og file
# df2['score'].value_counts().sort_index(ascending=False)


# In[ ]:





# In[ ]:





# In[8]:


df2.groupby(by=['score'])['match_num'].value_counts().sort_index(ascending=False)


# In[15]:


df2.groupby(by=['score'])['match_num'].value_counts().sort_index(ascending=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[9]:


df2['score'].value_counts().sort_index(ascending=False)


# In[9]:


df2['score'].value_counts().sort_index(ascending=False)


# In[8]:


df2['score'].value_counts().sort_index(ascending=False)


# In[8]:


df2['score'].value_counts().sort_index(ascending=False)


# In[9]:


df2['score'].value_counts().sort_index(ascending=False)


# In[8]:


df2['score'].value_counts().sort_index(ascending=False)


# In[10]:


df2['score'].value_counts().sort_index(ascending=False)


# In[ ]:





# In[9]:


df2['score'].value_counts().sort_index(ascending=False)


# In[14]:


# df2['score'].value_counts().sort_index(ascending=False)


# In[ ]:





# In[ ]:





# In[ ]:


# import seaborn as sns

# df5_100=df5.loc[df5['nameScores']==100]


# In[ ]:


# sns.set(rc = {'figure.figsize':(10,8)})
# sns.set_style("white")

# sns.histplot(data=df5_100, x="totalScore")


# In[ ]:


# sns.set(rc = {'figure.figsize':(10,8)})
# sns.set_style("white")

# sns.histplot(data=df5_100, x="totalScore")


# In[ ]:





# In[ ]:





# In[ ]:




