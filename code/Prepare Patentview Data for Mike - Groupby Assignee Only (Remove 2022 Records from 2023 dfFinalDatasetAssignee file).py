#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import time
import warnings
warnings.filterwarnings('ignore')

t0=time.time()

srcFiles="../csvResults"

mainFile=pd.read_csv(os.path.join(srcFiles,"dfFinalDatasetAssignee.csv"))

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(mainFile.info(null_counts=True),mainFile.head())


# In[ ]:





# In[2]:


t0=time.time()

srcFiles="../csvResults"

stateless=pd.read_csv(os.path.join(srcFiles,"2022PV2800StatelessxWalk.csv"),
                      usecols=['ID','assignee','organization_x'])
stateless.rename(columns={'organization_x':'organization'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(stateless.info(null_counts=True),stateless.head())


# In[ ]:





# In[3]:


t0=time.time()

mainStatelessMerge=mainFile.merge(stateless,on=['assignee'],how='outer',indicator=True)
mainStatelessMerge1=mainStatelessMerge.loc[mainStatelessMerge['_merge']=='left_only'].iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12]].copy()

mainStatelessMerge1.rename(columns={'ID_x':'ID','organization_x':'organization'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(mainStatelessMerge1.info(null_counts=True),mainStatelessMerge1.head())


# In[ ]:





# In[4]:


t0=time.time()

srcFiles="../csvResults"

state=pd.read_csv(os.path.join(srcFiles,"2022PV50000StatexWalk.csv"),
                  usecols=['ID','assignee','organization_x'])
state.rename(columns={'organization_x':'organization'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(state.info(null_counts=True),state.head())


# In[ ]:





# In[5]:


t0=time.time()

mainStatelessStateMerge=mainStatelessMerge1.merge(state,on=['assignee'],how='outer',indicator=True)
mainStatelessStateMerge1=mainStatelessStateMerge.loc[mainStatelessStateMerge['_merge']=='left_only'].iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12]].copy()

mainStatelessStateMerge1.rename(columns={'ID_x':'ID','organization_x':'organization'},inplace=True)

t1=time.time()
total=t1-t0
print("Total time is %4f" % (total/60), "mins\n")

display(mainStatelessStateMerge1.info(null_counts=True),mainStatelessStateMerge1.head())


# In[6]:


# mainStatelessStateMerge1.to_csv("../csvResults/dfFinalDatasetAssigneeRemoved2022OutputResults.csv",index=False)


# In[ ]:




