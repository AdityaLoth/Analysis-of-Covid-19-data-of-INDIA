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
dist=[]
males=[]
females=[]

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
        
        if(i%10==0):
            Total.append([state,dist,w,mon,a[i]])
        elif(i%10==5):
            males.append([state,dist,w,mon,a[i]])
        elif(i%10==6): 
            females.append([state,dist,w,mon,a[i]])

Malesdf=pd.DataFrame(males,columns=["State code","Dist code","Week ID","Month ID","Cases"])
Femalesdf=pd.DataFrame(females,columns=["State code","Dist code","Week ID","Month ID","Cases"])



Malesdf4=Malesdf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Femalesdf4=Femalesdf.groupby(["State code","Dist code"]).Cases.last().to_frame(name='Cases').dropna().reset_index()
Malesdf4=diff(Malesdf4)
Femalesdf4=diff(Femalesdf4)



census=pd.read_excel("dataset/census.xlsx")

census=census[["Name","TOT_P","TOT_M","TOT_F"]]
c1=census.groupby(["Name"]).TOT_P.first().to_frame(name="Total population").reset_index()

header=["Dist"]
withcode= pd.read_csv("dataset/distwithcode.csv",names=header)
withcode=withcode["Dist"].tolist()
nocode=pd.read_csv("dataset/distwithoutcode.csv",names=header)
nocode=nocode["Dist"].tolist()

def spellingcorrection(a):
    for j in range(len(wrong)):
        if(a==wrong[j].strip()):
            a=right[j].strip()
    return a

def clean(a):
    a= a.replace("_"," ")
    a=a.rstrip('/0123456789')
    a=a.replace(' district',"")
    a=a.replace(" ","")
    return a

correct=pd.read_excel("dataset/difference district.xlsx")
wrong=correct["neighbor json"]
right=correct["districtwise"]


def statecode(a):
    idx=nocode2.index(a.lower())
    return withcode[idx]

country=c1.loc[1]
idd = census.groupby(['Name'])['TOT_P'].transform('first') == census['TOT_P']
c1=census[idd].reset_index()
c2=c1[["Name","TOT_P","TOT_M","TOT_F"]]
c2=c2.drop_duplicates(subset=None, keep='first', inplace=False)
india=c2.loc[0]
c2=c2.loc[1:].reset_index().drop(columns='index')

ll1=[]
ccc=[]
nocode2=[]
for a in nocode:
    a=clean(a)
    nocode2.append(a)
    
for i in range(len(c2)):
    r=c2["Name"][i]
    r=clean(r)
    r=spellingcorrection(r)
    if r.lower() in nocode2:
        r =statecode(r)
        ll1.append([r,c2["TOT_P"][i],c2["TOT_M"][i],c2["TOT_F"][i]])
        ccc.append(r)

cen=pd.DataFrame(ll1,columns=["Dist code","Total","male","female"])

idy = cen.groupby(['Dist code'])['Total'].transform('first') == cen['Total']
cen2=cen[idy]
cen2=cen2.drop_duplicates(subset=None, keep='first', inplace=False)
cen2=cen2.sort_values('Dist code').reset_index()
cen2=cen2[["Dist code","Total","male","female"]]

cen2.to_csv("dataset/cen2.csv",index=False)

vacmf = [cen2,Malesdf4,Femalesdf4]
vacmf = reduce(lambda left,right: pd.merge(left,right,on=['Dist code']), vacmf)
vacmf=vacmf[["Dist code","Total","male","female","Cases_x","Cases_y"]]
vacmf.columns=["districtid","Total","male","female","vaccine male","vaccine female"]
vacmf["vaccinationratio"]=vacmf["vaccine female"]/vacmf["vaccine male"]
vacmf["populationratio"]=vacmf["female"]/vacmf["male"]
vacmf["ratioofratios"]=vacmf["vaccinationratio"]/vacmf["populationratio"]
ques6=vacmf[["districtid","vaccinationratio","populationratio","ratioofratios"]]

ques6.to_csv("district-vaccination-population-ratio.csv",index=False)

vacmf2=vacmf[["districtid","Total","male","female","vaccine male","vaccine female"]]
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
vacmf2=state(vacmf2)
vacmf2 = vacmf2.groupby(['stateid']).agg({'Total': 'sum', 'male': 'sum', 'female': 'sum', 'vaccine male': 'sum', 'vaccine female': 'sum'}).reset_index(drop=False)

vacmf2["vaccinationratio"]=vacmf2["vaccine female"]/vacmf2["vaccine male"]
vacmf2["populationratio"]=vacmf2["female"]/vacmf2["male"]
vacmf2["ratioofratios"]=vacmf2["vaccinationratio"]/vacmf2["populationratio"]
qq=vacmf2[["stateid","vaccinationratio","populationratio","ratioofratios"]]
qq.to_csv("state-vaccination-population-ratio.csv",index=False)

vacmf3=vacmf2[["stateid","Total","male","female","vaccine male","vaccine female"]]
vacmf3=vacmf2.agg({'Total': 'sum', 'male': 'sum', 'female': 'sum', 'vaccine male': 'sum', 'vaccine female': 'sum'})
vacmf3=pd.DataFrame(vacmf3)
qqq=vacmf3.T
qqq["vaccinationratio"]=qqq["vaccine female"]/qqq["vaccine male"]
qqq["populationratio"]=qqq["female"]/qqq["male"]
qqq["ratioofratios"]=qqq["vaccinationratio"]/qqq["populationratio"]
qqq=qqq[["vaccinationratio","populationratio","ratioofratios"]]

qqq.to_csv("overall-vaccination-population-ratio.csv",index=False)

