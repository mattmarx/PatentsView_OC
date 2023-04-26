#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import time
import warnings
warnings.filterwarnings('ignore')

t0=time.time()

srcFiles="../sourceFiles/"

df=pd.read_csv(os.path.join(srcFiles,"assignee.csv"),usecols=['rf_id','ee_name'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df.info(null_counts=True),df.head())


# In[ ]:





# In[2]:


t0=time.time()

df1=pd.read_csv(os.path.join(srcFiles,"assignor.csv"),usecols=['rf_id','or_name'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df1.info(null_counts=True),df1.head())


# In[ ]:





# In[3]:


t0=time.time()

merge1=df.merge(df1,on=['rf_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge1.info(null_counts=True),merge1.head())


# In[ ]:





# In[4]:


merge1.loc[merge1['rf_id']==12800340]


# In[ ]:





# In[5]:


t0=time.time()

df2=pd.read_csv(os.path.join(srcFiles,"assignment.csv"),usecols=['rf_id','record_dt'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df2.info(null_counts=True),df2.head())


# In[ ]:





# In[6]:


t0=time.time()

merge2=merge1.merge(df2,on=['rf_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge2.info(null_counts=True),merge2.head())


# In[ ]:





# In[7]:


merge2.loc[merge2['rf_id']==12800340]


# In[ ]:





# In[8]:


t0=time.time()

df3=pd.read_csv(os.path.join(srcFiles,"assignment_conveyance.csv"),usecols=['rf_id','convey_ty'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df3.info(null_counts=True),df3.head())


# In[ ]:





# In[9]:


t0=time.time()

merge3=merge2.merge(df3,on=['rf_id'],how='inner')

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(merge3.info(null_counts=True),merge3.head())


# In[ ]:





# In[10]:


merge3.loc[merge3['rf_id']==12800340]


# In[ ]:





# In[11]:


t0=time.time()

df4=pd.read_csv(os.path.join(srcFiles,"documentid.csv"),usecols=['rf_id','title','appno_doc_num','appno_date',
                                                                 'grant_doc_num','grant_date'])

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(df4.info(null_counts=True),df4.head())


# In[ ]:





# In[12]:


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


# In[ ]:





# In[13]:


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


# In[ ]:





# In[14]:


t0=time.time()

filterMerge4=merge4.loc[(merge4.convey_type=='assignment') | (merge4.convey_type=='correct') | 
                        (merge4.convey_type=='namechg') | (merge4.convey_type=='merger')].copy()
filterMerge4.sort_values(by=['rf_id','record_date'],inplace=True)

filterMerge5=filterMerge4.drop_duplicates(subset=['rf_id'],keep='first').copy()

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins")
print("There are",filterMerge5.rf_id.nunique(),"unique rf_ids\n")

display(filterMerge5.info(null_counts=True),filterMerge5.head())


# In[ ]:





# In[16]:


filterMerge5[filterMerge5.grant_num.str.contains('6327609',case=False,na=False,regex=True)]


# In[ ]:





# In[ ]:





# In[23]:


# filterMerge5.to_csv("../csvResults/reassignment.csv",index=False)

