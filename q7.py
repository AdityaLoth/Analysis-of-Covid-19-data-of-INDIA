import numpy as np
import pandas as pd
import time
import datetime
import math
from functools import reduce

def TS(s):
    if(len(s)>10):
        s=s[:-2]
    e = datetime.datetime.strptime(s,"%d/%m/%Y") 
    t = datetime.datetime.timestamp(e)
    return t

def monthid(s):
    if(len(s)>10):
        s=s[:-2]
    e = datetime.datetime.strptime(s,"%d/%m/%Y")
    m=8
    mon=e.month
    if(e.day>14): return m+mon+1
    return m+mon

def weekid(s):
    ts=TS(s)
    start = TS("14/03/2020")
    add=604800
    num= (ts-start)/add
    if(num<0): num=0

    return math.ceil(num)

def diff(file):
    file["Cases"]=pd.to_numeric(file["Cases"], errors='coerce')
    for i in reversed(range(1,len(file))):
      if file["Dist code"][i]==file["Dist code"][i-1]:
#         if file["Cases"][i-1]==None:
#             file["Cases"][i-1]=file["Cases"][i]
        file["Cases"][i]=max(0, file["Cases"][i]-file["Cases"][i-1])
        
    return file
    

file1=pd.read_csv("dataset/cowin_vaccine_data_districtwise.csv", index_col=False, dtype='unicode', low_memory=False)
file2=file1.drop(columns=["S No","State","Cowin Key","District"])

aa=[]
state=[]
dist=[]
Total=[]
firstdose=[]
seconddose=[]
males=[]
females=[]
trans=[]
covaxin=[]
covishield=[]

for row in file2.iterrows():
    aa=[]
    aa.append(row)
    aa=np.array(aa)
    aa=aa[0][1]
    
    state=aa[0]
    dist=aa[1]
    
    a=aa[2:]
    p=pd.DataFrame(a)
    dates= p.T.columns
    for i in range(len(a)):
        date=dates[i]
        if(TS(date)>TS("14/08/2021")): break
        w=weekid(date)
        mon=monthid(date)
        if(i%10==8): 
            covaxin.append([state,dist,w,mon,a[i]])
#             covaxin.append([state,dist,wodd,mon,a[i]])

        elif(i%10==9): 
            covishield.append([state,dist,w,mon,a[i]])

Covaxindf=pd.DataFrame(covaxin,columns=["State code","Dist code","Week ID","Month ID","Cases"])
Covidf=pd.DataFrame(covishield,columns=["State code","Dist code","Week ID","Month ID","Cases"])
Covaxindf4=Covaxindf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Covidf4=Covidf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Covaxindf4=diff(Covaxindf4)
Covidf4=diff(Covidf4)

cen2=pd.read_csv("dataset/cen2.csv")

q7=[cen2,Covaxindf4,Covidf4]
q7=reduce(lambda left,right: pd.merge(left,right,on=['Dist code']), q7)
q7a=q7[["Dist code","Cases_x","Cases_y"]]
q7a["vaccineratio"]=q7a["Cases_y"]/q7a["Cases_x"]
q7a=q7a[["Dist code","vaccineratio"]]
q7a.columns=["districtid","vaccineratio"]
q7a.replace([np.inf, -np.inf], np.nan, inplace=True)
q7a=q7a.sort_values('vaccineratio').reset_index()
q7a=q7a[["districtid","vaccineratio"]]

q7a.to_csv("district-vaccine-type-ratio.csv",index=False)

def state(s):
    mon=s
    stateid=[]
    for i in range(len(mon)):
        x=mon["districtid"][i]
        x=x[:2]
        stateid.append(x)

    stateid=pd.DataFrame(stateid)
    s["stateid"]=stateid
    return s

q7b=q7[["Dist code","Cases_x","Cases_y"]]
q7b.columns=["districtid","covaxine","covishield"]
q7b=state(q7b)
q7b=q7b.groupby(['stateid']).agg({'covaxine': 'sum', 'covishield': 'sum'}).reset_index(drop=False)
q7c=q7b[["stateid","covaxine","covishield"]]
q7b["vaccineratio"]=q7b["covishield"]/q7b["covaxine"]
q7b=q7b[["stateid","vaccineratio"]]
q7b.replace([np.inf, -np.inf], np.nan, inplace=True)
q7b=q7b.sort_values('vaccineratio').reset_index()
q7b=q7b[["stateid","vaccineratio"]]
q7b.to_csv("state-vaccine-type-ratio.csv",index=False)

q7c=q7c.agg({'covaxine': 'sum', 'covishield': 'sum'})
q7c=pd.DataFrame(q7c)
q7d=q7c.T
q7d["vaccineratio"]=q7d["covishield"]/q7d["covaxine"]
q7d=q7d[["vaccineratio"]]
q7d.replace([np.inf, -np.inf], np.nan, inplace=True)
q7d=q7d.sort_values('vaccineratio').reset_index()
q7d=q7d[["vaccineratio"]]
q7d.to_csv("overall-vaccine-type-ratio.csv",index=False)
