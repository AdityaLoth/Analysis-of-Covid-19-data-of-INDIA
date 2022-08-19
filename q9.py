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
firstdose=[]

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

        if(i%10==3): 
            firstdose.append([state,dist,w,mon,a[i]])
            
Firstdf=pd.DataFrame(firstdose,columns=["State code","Dist code","Week ID","Month ID","Cases"])
Firstdf4=Firstdf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Firstdf4=diff(Firstdf4)

cen2=pd.read_csv("dataset/cen2.csv")
day=TS("14/08/2021")-TS("13/08/2021")
days=(TS("14/08/2021")-TS("15/01/2021"))/(day)

q9=[cen2,Firstdf4]
q9=reduce(lambda left,right: pd.merge(left,right,on=['Dist code']), q9)
q9=q9.drop(columns=['Dist code','male','female'])
q9=q9.groupby("State code").sum().reset_index()
q9["populationleft"]=q9["Total"]-q9["Cases"]
q9["populationleft"][q9["populationleft"]<0]=0
q9["rateofvaccination"]=q9["Cases"]/days

q9date=[]
for w in range(len(q9)):
        ww=max(0,np.floor(q9["populationleft"][w]/q9["rateofvaccination"][w])*day) +TS("14/08/2021")
        ret=datetime.datetime.fromtimestamp(ww, tz=None).strftime('%d-%m-%y')
        q9date.append(ret)
q9["Date"]=pd.DataFrame(q9date)
q9=q9.drop(columns=["Total","Cases"])
q9.columns=["stateid","populationleft","rateofvaccination","Date"]
q9.to_csv("complete-vaccination.csv",index=False)

