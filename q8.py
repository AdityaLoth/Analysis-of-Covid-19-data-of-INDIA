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
Total=[]
firstdose=[]
seconddose=[]

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
        
#         if(i%10==0):
#             Total.append([state,dist,weven,mon,a[i]])
#             Total.append([state,dist,wodd,mon,a[i]])
        if(i%10==3): 
            firstdose.append([state,dist,w,mon,a[i]])
#             firstdose.append([state,dist,wodd,mon,a[i]])
        elif(i%10==4): 
            seconddose.append([state,dist,w,mon,a[i]])

Firstdf=pd.DataFrame(firstdose,columns=["State code","Dist code","Week ID","Month ID","Cases"])
Seconddf=pd.DataFrame(seconddose,columns=["State code","Dist code","Week ID","Month ID","Cases"])

Firstdf4=Firstdf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Seconddf4=Seconddf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Firstdf4=diff(Firstdf4)
Seconddf4=diff(Seconddf4)


cen2=pd.read_csv("dataset/cen2.csv")

q8=[cen2,Firstdf4,Seconddf4]
q8=reduce(lambda left,right: pd.merge(left,right,on=['Dist code']), q8)

q8=q8[["Dist code","Total","Cases_x","Cases_y"]]
q8["vaccinateddose1ratio"]=q8["Cases_x"]/q8["Total"]
q8["vaccinateddose2ratio"]=q8["Cases_y"]/q8["Total"]

q8x=q8[["Dist code","vaccinateddose1ratio","vaccinateddose2ratio"]]
q8x.columns=["districtid","vaccinateddose1ratio","vaccinateddose2ratio"]
q8x.replace([np.inf, -np.inf], np.nan, inplace=True)
q8x.sort_values('vaccinateddose1ratio')
q8x.to_csv("district-vaccinated-dose-ratio.csv",index=False)

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
q8y=q8[["Dist code","Total","Cases_x","Cases_y"]]
q8y.columns=["districtid","Total","covaxine","covishield"]
q8y=state(q8y)
q8y=q8y.groupby(['stateid']).agg({'Total':'sum','covaxine': 'sum', 'covishield': 'sum'}).reset_index(drop=False)
q8y=q8y[["stateid","Total","covaxine","covishield"]]
q8y["vaccinateddose1ratio"]=q8y["covaxine"]/q8y["Total"]
q8y["vaccinateddose2ratio"]=q8y["covishield"]/q8y["Total"]
q8y=q8y[["stateid","vaccinateddose1ratio","vaccinateddose2ratio"]]
q8y.replace([np.inf, -np.inf], np.nan, inplace=True)
q8y.sort_values('vaccinateddose1ratio')
q8y.to_csv("state-vaccinated-dose-ratio.csv",index=False)

q8z=q8[["Dist code","Total","Cases_x","Cases_y"]]
q8z.columns=["districtid","Total","covaxine","covishield"]
q8z=q8z.agg({'Total':'sum','covaxine': 'sum', 'covishield': 'sum'})
q8z=pd.DataFrame(q8z)
q8z=q8z.T
q8z["vaccinateddose1ratio"]=q8z["covaxine"]/q8z["Total"]
q8z["vaccinateddose2ratio"]=q8z["covishield"]/q8z["Total"]
q8z=q8z[["vaccinateddose1ratio","vaccinateddose2ratio"]]
q8z.to_csv("overall-vaccinated-dose-ratio.csv",index=False)

