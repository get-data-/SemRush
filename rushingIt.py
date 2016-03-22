# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 09:15:35 2016

@author: ktarvin
"""

import ijson, requests, json, os, io
import pandas as pd
import time, datetime

    
def majorKey():
    with open(##FP_Here##, 'r') as f:
        ApiKey = f.read()
        return ApiKey
def clientProject():
    clientDF = pd.read_csv(##FP_Here##)
    return clientDF

def queryBuilder(sDate,eDate,projectId,projectUrl,ApiKey):
    pId = unicode(projectId)
    apiKey = u'?key='+unicode(ApiKey)
    api = u'http://api.semrush.com/reports/v1/projects/'+pId+u'/tracking/'
    action = u'&action=report'
    rType = u'&type=tracking_position_organic' #'tracking_visibility_organic'
    url = u'&url='+unicode(projectUrl)
    d_begin = u'&date_begin='+unicode(sDate) 
    d_end = u'&date_end='+unicode(eDate) 
    d_limit = u'&display_limit='+u'100'
    d_offset = u'&display_offset='+u'0'
    #display_tags = #Tags Separated by |
    l_filter = u'&linktype_filter='+u'0' # 0=include local pack, 1=show only local 2=exclude
    query = api+apiKey+action+rType+d_limit+d_offset+d_begin+d_end+url+l_filter
    return query

def sendQuery(query):
    r = requests.get(query)
    response = r.json()
    return response

def outputResponse(ofilePath, response):
    with io.open(ofilePath, 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(response, ensure_ascii=False)))


def parseThe(jsonFile, projectUrl):
    with open(jsonFile, 'r') as f:
        obj = ijson.items(f, 'data')
        columns = list(obj)
    kwdata = [col for col in columns]
    rank_headers = ['Keyword','Keyword_Tags','Search_Volume','CPC','Date','Keyword_Position', 'Landing_Page']
    rank_df = []
    for data in kwdata:
        for meta in data.keys():
            kw = data[meta]['Ph']
            kw_v = data[meta]['Nq']
            kw_cpc = data[meta]['Cp']
            kw_tag = data[meta]['Tg'].values()
            for thedate in data[meta]['Dt']:
                date = datetime.datetime.strptime(thedate, '%Y%m%d').date()
                rank = data[meta]['Dt'][thedate][projectUrl]
                for otherdate in data[meta]['Lu']:
                    if otherdate == thedate:
                        url = data[meta]['Lu'][otherdate][projectUrl]
                        rankdata = [kw, kw_tag, kw_v, kw_cpc, date, rank, url]
                        rank_df.append(rankdata)
                    else:
                        pass
    return rank_headers, rank_df

def structure(data, headers):
    df = pd.DataFrame(data, columns=headers)
    df.set_index('Keyword', inplace=True)
    return df

try:
    sDate = raw_input('Please Enter Start Date (YYYYMMDD): ')
    eDate = raw_input('Please Enter End Date (YYYYMMDD): ')
    clientDF = clientProject()
    for i in clientDF.index:
        pName = clientDF['project_name'][i]
        projectId = unicode(clientDF['project_id'][i])
        projectUrl = clientDF['url'][i]
        jsonFile = ##FP_Here## + pName + '.json'
        ofilePath = os.path.normpath(jsonFile)
        query = queryBuilder(sDate,eDate,projectId,projectUrl,majorKey())
        time.sleep(1)
        response = sendQuery(query)
        outputResponse(ofilePath, response)
        rank_headers, rank_df = parseThe(jsonFile, projectUrl)
        df = structure(rank_df, rank_headers)
        fn = pName + '_rank.csv'
        df.to_csv(fn, encoding='utf-8')
except Exception as e:
    print e
