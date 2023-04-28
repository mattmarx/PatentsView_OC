import pandas as pd
import numpy as np
import time
import os
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')

"""
Author: Joshua Chu
Date: April 23, 2023

Description: this script scores the PatentsView-OC file that was prepared in the
'Merge OpenCorporate Results with Input Data and Clean for Scoring.py' script. There is
no date prep steps in this script, only scoring. If addtional data prep steps must be
performed, the 'Merge OpenCorporate Results with Input Data and Clean for Scoring.py'
script should be modified to include those steps.

"""

### the code below imports the file to be scored and converts the date fields
### to the necessary data types
t0=time.time()

### set the path for the input file and save to variable
res_folder = "../csvResults/"
input_file = "readyForScoring.csv"

input_directory=os.path.join(res_folder,input_file)
print(input_directory,"\n")

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

print(df.info(null_counts=True),flush=True)
print(df.head(),flush=True)


### the scoring algorithm is below. A short description for each section is provided
### to understand the steps
t0=time.time()

totalScore=[]
d=len(df)
x=''

for s in range(d):

    ### scores all records with a fuzzy match score with a 100%; account for organization
    ### name lengths (i.e., shorter names are scored lower than longer names). There should
    ### be no org names with less than 5 characters, but if any squeaked through, make sure
    ### they are penalized
    if df['nameScores'][s] == 100:

        if len(df['organization'][s]) < 5:
            x=0

        elif len(df['organization'][s]) >= 5:
            x=5

    ### visual inspection of the data indicates discrete groups exist, which results in
    ### the next section. Name lengths are again considered where shorter names/scores
    ### are downweighted
    elif 90 <= df['nameScores'][s] < 100:

        if len(df['organization'][s]) < 5:
            x=0

        elif 5 <= len(df['organization'][s]) < 10:
            x=1

        elif 10 <= len(df['organization'][s]) < 15:
            x=2

        elif len(df['organization'][s]) >= 15:
            x=5

    ### this section and below could probably use some optimizing. The existing
    ### fuzzy score ranges were manually determined early on in the project and
    ### likely be refined
    elif 87 <= df['nameScores'][s] < 90:

        if len(df['organization'][s]) < 5:
            x=-1

        elif 5 <= len(df['organization'][s]) < 10:
            x=1

        elif 10 <= len(df['organization'][s]) < 15:
            x=2

        elif len(df['organization'][s]) >= 15:
            x=4

    elif df['nameScores'][s] < 87:
        x=-7

    elif np.isnan(df['nameScores'][s]):
        x=-10

    ### scoring the city-state pairs for each organization. A new variable 'a' is
    ### included in this section. The purpose of this variable is to prevent records
    ### from getting points in this section for an exact city-state pair match and in
    ### coordinates scoring section below
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

    ###IMPORTANT: THIS SHOULD BE CHANGED BECAUSE WE NOW CALCULATE DATE DIFFERENCES USING ASSIGNEE DATA###

    if df['dateFiledMin'][s] < df['incorporation_date'][s]:

        if df['dateDiff'][s] >= -5 and df['dateDiff'][s] < 0:
            x=x-1

        elif df['dateDiff'][s] >= -10 and df['dateDiff'][s] < -5:
            x=x-3

        elif df['dateDiff'][s] < -10:
            x=x-5

    ### if the variable 'a' is set to 1, the record is skipped and moves to the date difference
    ### scoring section below. If 'a' is not 1, the city-city distance that was calculated in the
    ### 'Merge OpenCorporate Results with Input Data and Clean for Scoring.py' script is utilized
    ### to score the record using distance ranges set below. If no distance was able to be determined
    ### a score of zero is given. It is possible this section could be refined
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

    ### this final section utilizes the dateDiff field and scores the record
    ### according to the corresponding ranges. There is an opportunity to
    ### modify these ranges to move lower scoring records to a 'higher' level
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


### append the existing data frame with the scores for each record
t0=time.time()

df['totalScore'] = totalScore

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df.assignee.nunique())
print("The number of unique IDs are:",df.ID.nunique(),"\n")

print(df.info(),flush=True)
print(df.head(),flush=True)

df.to_csv("../csvResults/scoredOCResults.csv",index=False)
os.chmod("../csvResults/scoredOCResults.csv",0o777)


### calculate the confidence scores using the totalScores from the previous section
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

print(df.info(),flush=True)
print(df.head(),flush=True)


### convert the confidence scores from floats to integers by using the floor
### function, which moves each score to the next lowest integer, not the next
### highest integer. After this step, the values are sorted using the fields
### below and applying different ascending options
t0=time.time()

df['score'] = df['confidenceScore'].apply(np.floor)
df1=df.sort_values(by=['ID','score','dateDiff','record_date'],ascending=[True,False,False,False]).reset_index(drop=True)

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")

print(df1.info(),flush=True)
print(df1.head(),flush=True)


### drop duplicates in the data set. By using the sorting options in the
### previous section, the goal is to drop by ID and keeping the first. This
### should result in the highest scoring record for each assignee
t0=time.time()

df2=df1.drop_duplicates(subset=['ID'],keep='first')

### end timer and print total time
t1=time.time()
total=t1-t0
print("Total time is %.3f" % (total/60), "mins\n")
print("The number of unique assignee IDs are:",df2.assignee_id.nunique())
print("The number of unique IDs are:",df2.ID.nunique(),"\n")

print(df2.info(),flush=True)
print(df2.head(),flush=True)


df2.to_csv("../csvResults/reviewScoredResults.csv",index=False)
os.chmod("../csvResults/reviewScoredResults.csv",0o777)
