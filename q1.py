import numpy as np
import pandas as pd

neighbor= pd.read_csv("dataset/neighbor-districts.csv", dtype='unicode', low_memory=False)
vaccination=pd.read_csv("dataset/cowin_vaccine_data_districtwise.csv", dtype='unicode', low_memory=False)


mainlist=vaccination["District_Key"]
main=mainlist[1:]
seclist=vaccination["District"]
sec=seclist[1:]

sec2=[]
main2=[]
drop=['Chengalpattu', 'Gaurela Pendra Marwahi', 'Nicobars', 'North and Middle Andaman', 'Saraikela-Kharsawan',
'South Andaman', 'Tenkasi', 'Tirupathur', 'Yanam','Kheri', 'Konkan division', 'Niwari', 'Noklak', 'Parbhani', 'Pattanamtitta']
for i in range(1,len(sec)+1):
    if sec[i] not in drop:
        sec2.append(sec[i])
        main2.append(main[i])
        
def clean(a):
    a= a.replace("_"," ")
    a=a.rstrip('/0123456789')
    a=a[:-2]
    a=a.replace(' district',"")
    return a

edit1=neighbor.columns
edit2=list()

for i in edit1:
    c= clean(i)
    edit2.append(c)
    
def spellingcorrection(a):
    for j in range(len(wrong)):
        if(a==wrong[j].strip()):
            a=right[j].strip()
    return a

correct=pd.read_excel("dataset/difference district.xlsx")
wrong=correct["neighbor json"]
right=correct["districtwise"]

for i in range(len(edit2)):
    for j in range(len(wrong)):
        if(edit2[i]==wrong[j].strip()):
            edit2[i]=right[j].strip()
        
edit3=list()
edit4=list()
for i in range(len(edit2)):
#     edit2[i]=edit2[i].capitalize()
    for j in range(len(sec2)):
        if(edit2[i]==sec2[j].lower()):
#             edit2[i]=mainlist[j+1]
            edit3.append(main2[j])
            edit4.append(edit2[i])
            break

out1=edit3
out2=edit4
edit3=pd.Series(edit3).drop_duplicates().reset_index(drop=True)
edit4=pd.Series(edit4).drop_duplicates().reset_index(drop=True)

np.savetxt("dataset/distwithcode.csv", out1, delimiter=",", fmt='%s')
np.savetxt("dataset/distwithoutcode.csv", out2, delimiter=",", fmt='%s')

copynb=neighbor
dict1= copynb.to_dict('list')
from collections import defaultdict
dict2=defaultdict(list)


def statecode(a):
    flag=0
    for i in range(len(edit3)):
        if(a==edit4[i]):
            a=edit3[i]
            flag=1
            break
    return a,flag
    

for key, value in dict1.items():
    flag=0
    key=clean(key)
    key=spellingcorrection(key)
    value = [x for x in value if pd.isnull(x) == False]
    v=list()
    for val in value:
        f=0
#         if(np.isnan(val)==True):
#             break
        val=clean(val)
        val=spellingcorrection(val)
        val,f=statecode(val)
        if(f):
            v.append(val)
    key,flag=statecode(key)
    if(flag):
        dict2[key].extend(v)

keys = sorted(dict2)
dict3 = {x:dict2[x] for x in keys}


import json
with open("neighbor-districts-modified.json", "w") as outfile:
    json.dump(dict3, outfile)

np.savetxt("dataset/distwithcode.csv", edit3, delimiter=",", fmt='%s')
np.savetxt("dataset/distwithoutcode.csv", edit4, delimiter=",", fmt='%s')
