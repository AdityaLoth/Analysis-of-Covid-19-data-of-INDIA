import numpy as np
import pandas as pd
import time
import datetime
import math

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

file1=pd.read_csv("dataset/cowin_vaccine_data_districtwise.csv", index_col=False, dtype='unicode', low_memory=False)
file2=file1.drop(columns=["S No","State","Cowin Key","District"])


aa=[]
state=[]
dist=[]
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
        w= weekid(date)
        mon=monthid(date)
        
        if(i%10==3): 
            firstdose.append([state,dist,w,mon,a[i]])
        elif(i%10==4): 
            seconddose.append([state,dist,w,mon,a[i]])
        
def diff(file):
    file["Cases"]=pd.to_numeric(file["Cases"], errors='coerce')
    for i in reversed(range(1,len(file))):
      if file["Dist code"][i]==file["Dist code"][i-1]:
        file["Cases"][i]=max(0, file["Cases"][i]-file["Cases"][i-1])
        
    return file

Firstdf=pd.DataFrame(firstdose,columns=["State code","Dist code","Week ID","Month ID","Cases"])
Seconddf=pd.DataFrame(seconddose,columns=["State code","Dist code","Week ID","Month ID","Cases"])

Firstdf2=Firstdf.groupby(["State code","Dist code","Week ID"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Seconddf2=Seconddf.groupby(["State code","Dist code","Week ID"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Firstdf2=diff(Firstdf2)
Seconddf2=diff(Seconddf2)

from functools import reduce
dose12 = [Firstdf2,Seconddf2]
dose12 = reduce(lambda left,right: pd.merge(left,right,on=['Dist code','State code','Week ID']), dose12)

q5=dose12[["Dist code","Week ID","Cases_x","Cases_y"]]
q5.columns=["districtid","weekid","dose 1","dose 2"]
q5.to_csv("district-vaccinated-count-week.csv",index=False)

q5a=dose12[["State code","Week ID","Cases_x","Cases_y"]]
q5a.columns=["stateid","weekid","dose 1","dose 2"]
q5a=q5a.groupby(['stateid','weekid']).agg({'dose 1':'sum','dose 2': 'sum'}).reset_index(drop=False)
q5a.to_csv("state-vaccinated-count-week.csv",index=False)

Firstdf3=Firstdf.groupby(["State code","Dist code","Month ID"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Seconddf3=Seconddf.groupby(["State code","Dist code","Month ID"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Firstdf3=diff(Firstdf3)
Seconddf3=diff(Seconddf3)

dose12x= [Firstdf3,Seconddf3]
dose12x = reduce(lambda left,right: pd.merge(left,right,on=['Dist code','State code','Month ID']), dose12x)

q5x=dose12x[["Dist code","Month ID","Cases_x","Cases_y"]]
q5x.columns=["districtid","monthid","dose 1","dose 2"]
q5x=q5x.to_csv("district-vaccinated-count-month.csv",index=False)

q5xb=dose12x[["State code","Month ID","Cases_x","Cases_y"]]
q5xb.columns=["stateid","monthid","dose 1","dose 2"]
q5xb=q5xb.groupby(['stateid','monthid']).agg({'dose 1':'sum','dose 2': 'sum'}).reset_index(drop=False)
q5xb.to_csv("state-vaccinated-count-month.csv",index=False)


Firstdf4=Firstdf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Seconddf4=Seconddf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Firstdf4=diff(Firstdf4)
Seconddf4=diff(Seconddf4)

dose12xx= [Firstdf4,Seconddf4]
dose12xx = reduce(lambda left,right: pd.merge(left,right,on=['Dist code','State code']), dose12xx)

q5xx=dose12xx[["Dist code","Cases_x","Cases_y"]]
q5xx.columns=["districtid","dose 1","dose 2"]
q5xx.to_csv("district-vaccinated-count-overall.csv",index=False)

q5xc=dose12xx[["State code","Cases_x","Cases_y"]]
q5xc.columns=["stateid","dose 1","dose 2"]
q5xc=q5xc.groupby(['stateid']).agg({'dose 1':'sum','dose 2': 'sum'}).reset_index(drop=False)
q5xc.to_csv("state-vaccinated-count-overall.csv",index=False)


