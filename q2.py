import pandas as pd
import numpy as np

df=pd.read_json("neighbor-districts-modified.json",lines=True,orient='columns')

header=["Dist"]
withcode= pd.read_csv("dataset/distwithcode.csv",names=header, dtype='unicode', low_memory=False)
withcode=withcode["Dist"].tolist()
withcode.sort()
nocode=pd.read_csv("dataset/distwithoutcode.csv",names=header, dtype='unicode', low_memory=False)
nocode=nocode["Dist"].tolist()

l2=list()
l=[]
i=1
for j in df.iteritems():
    for k in j[1][0]:
        idx= withcode.index(k)
        pair=(i,idx+1)
        p2=(j[0],k)
        inv=(idx+1,i)
        if inv in l:
            continue
        l.append(pair)
        l2.append(p2)
    i+=1

l2=pd.Series(l2)
l2.to_csv("edge-graph.csv",index=False)
