import pandas as pd
clientDF = pd.read_csv(##fp_here##)
newIndex = []
frames = []
for i in clientDF.index:
    pName = clientDF['project_name'][i]
    fn = pName + '_rank.csv'
    client = pd.read_csv(fn, index_col='Keyword', encoding='utf-8')
    frames.append(client)
    newIndex.append(clientDF['project_name'][i])
result = pd.concat(frames, keys=newIndex)
result.to_csv(##fp_here##, encoding='utf-8')
