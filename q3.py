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

def weekid(s):
    ts,_=TS(s)
    start,_ = TS("2020-03-14")
    add=604800
    num= (ts-start)/add
    if(num<0): num=0

    return math.ceil(num)

l=list()

for i in range(len(file2)):
    a=file2["District"][i]
    if a.lower() in nocode:
        a =statecode(a)
        w = weekid(file2["Date"][i])
        cases=file2["Confirmed"][i]
        l.append([a, w ,cases])

file3=pd.DataFrame(l,columns=["District ID","Week ID","Cases"])
file4=file3.groupby(["District ID","Week ID"]).Cases.last().to_frame(name='Cases').reset_index()

for i in reversed(range(1,len(file4))):
  if file4["District ID"][i]==file4["District ID"][i-1]:
    file4["Cases"][i]=max(0,file4["Cases"][i]-file4["Cases"][i-1])
file4.to_csv("cases-week.csv")


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
file7.to_csv("cases-month.csv")


file8=file6.groupby(["District ID"]).Cases.last().to_frame(name='Cases').reset_index()    
file8.to_csv("cases-overall.csv")

