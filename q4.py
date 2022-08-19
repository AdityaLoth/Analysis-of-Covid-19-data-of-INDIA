import numpy as np
import pandas as pd
import time
import datetime
import math

file1= pd.read_csv("dataset/districts.csv")
file2= file1[["Date","District","Confirmed"]]
header=["Dist"]
withcode= pd.read_csv("dataset/distwithcode.csv",names=header, dtype='unicode', low_memory=False)
withcode=withcode["Dist"].tolist()
nocode=pd.read_csv("dataset/distwithoutcode.csv",names=header, dtype='unicode', low_memory=False)
nocode=nocode["Dist"].tolist()

def statecode(a):
    idx=nocode.index(a.lower())
    return withcode[idx]


def TS(str):
    e = datetime.datetime.strptime(str,"%Y-%m-%d") 
    t = datetime.datetime.timestamp(e)
    return t,e.day


def weekidodd(s):
    ts,_=TS(s)
    start,_ = TS("2020-03-15")
    add=604800
    num= (ts-start)/add
    return math.floor(num)

def weekideven(s):
    ts,_=TS(s)
    start,_ = TS("2020-03-18")
    add=604800
    num= (ts-start)/add
    if(num<0): num=0

    return math.ceil(num)
    

l=list()

for i in range(len(file2)):
    a=file2["District"][i]
    if a.lower() in nocode:
        a =statecode(a)
        odd=1+ 2*weekidodd(file2["Date"][i])
        even=2*weekideven(file2["Date"][i])
        cases=file2["Confirmed"][i]
        l.append([a, odd, cases])
        l.append([a, even, cases])

file3=pd.DataFrame(l,columns=["District ID","Week ID","Cases"])
file4=file3.groupby(["District ID","Week ID"]).Cases.last().to_frame(name='Cases').reset_index()

for i in reversed(range(1,len(file4))):
  if file4["District ID"][i]==file4["District ID"][i-1]:
    file4["Cases"][i]=max(0,file4["Cases"][i]-file4["Cases"][i-1])


month=2
l2=list()
i=0
#  for i in range(len(file2)):
while(i!=len(file2)):
      # a=file2["District"][i]
      date=file2["Date"][i]
      # c=file2["Cases"]
      _,d=TS(date)
      if(d==15):
          month+=1
          while(d!=16):
            a=file2["District"][i]
          
            c=file2["Confirmed"]
          
            if a.lower() in nocode:
               a=statecode(a)
               l2.append([a,month,c])
            i+=1
            date=file2["Date"][i]
            _,d=TS(date)
      else:
        a=file2["District"][i]
          
        c=file2["Confirmed"][i]
          
        if a.lower() in nocode:
              a =statecode(a)
              l2.append([a,month,c])
        i+=1        


file5=pd.DataFrame(l2,columns=["District ID","Month ID","Cases"])
file6=file5.sort_values(["District ID","Month ID"],ignore_index=True)
file7=file6.groupby(["District ID","Month ID"]).Cases.last().to_frame(name='Cases').reset_index()
for i in reversed(range(1,len(file7))):
  if file7["District ID"][i]==file7["District ID"][i-1]:
    file7["Cases"][i]=max(0,file7["Cases"][i]-file7["Cases"][i-1])

#file7.to_csv("cases-month.csv")


file8=file6.groupby(["District ID"]).Cases.last().to_frame(name='Cases').reset_index()    
#file8.to_csv("cases-overall.csv")


week=file4
month=file7
overall=file8

w1month=list(range(1,5))
w1week=list(range(1,44))
w2month=list(range(13,15))
w2week=list(range(113,130))

l1=list()
l2=list()
for i in range(len(week)):
    if(week["Week ID"][i] in w1week):
        l1.append(week.loc[i])
    if(week["Week ID"][i] in w2week):
        l2.append(week.loc[i])


f1=pd.DataFrame(l1).reset_index(drop=True)
idx = f1.groupby(['District ID'])['Cases'].transform(max) == f1['Cases']
f2=f1[idx].reset_index(drop=True)
idxx = f2.groupby(['District ID','Cases'])['Week ID'].transform('first') == f2['Week ID']
f2=f2[idxx].reset_index(drop=True)
f2=f2[f2["Cases"]>0].reset_index(drop=True)


f3=pd.DataFrame(l2).reset_index(drop=True)
idx2=f3.groupby(['District ID'])['Cases'].transform(max) == f3['Cases']
f4=f3[idx2].reset_index(drop=True)
idx = f4.groupby(['District ID','Cases'])['Week ID'].transform('first') == f4['Week ID']
f4=f4[idx].reset_index(drop=True)
f4=f4[f4["Cases"]>0].reset_index(drop=True)


l3=list()
l4=list()
for j in range(len(month)):
    if(month["Month ID"][j] in w1month):
        l3.append(month.loc[j])
    if(month["Month ID"][j] in w2month):
        l4.append(month.loc[j])

f5=pd.DataFrame(l3).reset_index(drop=True)
idx3 = f5.groupby(['District ID'])['Cases'].transform(max) == f5['Cases']
f6=f5[idx3].reset_index(drop=True)
idxx = f6.groupby(['District ID','Cases'])['Month ID'].transform('first') == f6['Month ID']
f6=f6[idxx].reset_index(drop=True)
f6=f6[f6["Cases"]>0].reset_index(drop=True)


f7=pd.DataFrame(l4).reset_index(drop=True)
idx4 = f7.groupby(['District ID'])['Cases'].transform(max) == f7['Cases']
f8=f7[idx4].reset_index(drop=True)


from functools import reduce
dfs = [f2, f4, f6, f8]
df_final = reduce(lambda left,right: pd.merge(left,right,on='District ID'), dfs)
df_final=df_final[["District ID","Week ID_x","Week ID_y","Month ID_x","Month ID_y"]]

df_final.columns=[ 'districtid',
'wave1 − weekid', 'wave2 − weekid', 'wave1 − monthid', 'wave2 − monthid']
df_final.to_csv("district-peaks.csv",index=False)



def state(s):
    mon=s
    stateid=[]
    for i in range(len(mon)):
        x=mon["District ID"][i]
        x=x[:2]
        stateid.append(x)

    stateid=pd.DataFrame(stateid)
    s["stateid"]=stateid
    return s
    
file1x=state(f2)
file3x=state(f4)
file5x=state(f6)
file7x=state(f8)

file1x=file1x.groupby(["stateid","Week ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file1x.groupby(['stateid'])['Cases'].transform('max') == file1x['Cases']
file1x=file1x[idd].reset_index().drop(columns=["index","Cases"])

# file3x=file3x.groupby(["stateid","Week ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file3x.groupby(['stateid'])['Cases'].transform('max') == file3x['Cases']
file3x=file3x[idd].reset_index().drop(columns=["index","Cases"])

# file5x=file5x.groupby(["stateid","Month ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file5x.groupby(['stateid'])['Cases'].transform('max') == file5x['Cases']
file5x=file5x[idd].reset_index().drop(columns=["index","Cases"])

# file7x=file7x.groupby(["stateid","Month ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file7x.groupby(['stateid'])['Cases'].transform('max') == file7x['Cases']
file7x=file7x[idd].reset_index().drop(columns=["index","Cases"])

dff = [file1x, file3x, file5x, file7x]
dff = reduce(lambda left,right: pd.merge(left,right,on='stateid'), dff)
dff=dff[["stateid","Week ID_x","Week ID_y","Month ID_x","Month ID_y"]].drop_duplicates(keep='last').reset_index(drop=True)
dff.columns=["stateid","wave1 − weekid", "wave2 − weekid", "wave1 − monthid", "wave2 − monthid"]

dff.to_csv("state-peaks.csv",index=False)




file1y=f1.groupby(["Week ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file1y['Cases'].idxmax()
file1y=file1y.iloc[idd]

file3y=f3.groupby(["Week ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file3y['Cases'].idxmax()
file3y=file3y.iloc[idd]

file5y=f5.groupby(["Month ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file5y['Cases'].idxmax()
file5y=file5y.iloc[idd]

file7y=f7.groupby(["Month ID"]).Cases.sum().to_frame(name='Cases').reset_index()
idd = file7y['Cases'].idxmax()
file7y=file7y.iloc[idd]

dfy = [file1y["Week ID"], file3y["Week ID"], file5y["Month ID"], file7y["Month ID"]]
dfy=pd.DataFrame(dfy).T
dfy.columns=["wave1 − weekid", "wave2 − weekid", "wave1 − monthid", "wave2 − monthid"]
dfy.to_csv("overall-peaks.csv")
