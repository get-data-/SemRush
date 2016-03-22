# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 09:15:35 2016

@author: ktarvin
"""

import ijson, requests, json, os, io
import pandas as pd
import datetime


def majorKey():
    with open(##fp_here##, 'r') as f:
        ApiKey = f.read()
        return ApiKey
def clientProject():
    clientDF = pd.read_csv(##fp_here##)
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

def parseThe(jsonFile):
    q_site = #random_static_var
    with open(jsonFile, 'r') as f:
        obj = ijson.items(f, 'data')
        columns = list(obj)
    kwdata = [col for col in columns]
    rank_headers = ['Keyword','Search_Volume','CPC','Keyword_Tags']
    landing_headers = ['Keyword','Search_Volume','CPC','Keyword_Tags']
    rank_df = []
    landing_df = []
    for data in kwdata:
        for meta in data.keys():
            kw = data[meta]['Ph']
            kw_v = data[meta]['Nq']
            kw_cpc = data[meta]['Cp']
            kw_tag = data[meta]['Tg'].values()
            rank_row = [kw, kw_v, kw_cpc, kw_tag]        
            for thedate in data[meta]['Dt']:
                rd = datetime.datetime.strptime(thedate, '%Y%m%d').date()
                if rd in rank_headers:
                    pass
                else:
                    rank_headers.append(rd)
                rank = data[meta]['Dt'][thedate][q_site]
                rank_row.append(rank)
            rank_df.append(rank_row)
            landing_row = [kw, kw_v, kw_cpc, kw_tag]
            for l_date in data[meta]['Lu']:
                ld = datetime.datetime.strptime(l_date, '%Y%m%d').date()
                if ld in landing_headers:
                    pass
                else:
                    landing_headers.append(ld)
                landing_url = data[meta]['Lu'][l_date][q_site]
                landing_row.append(landing_url)
            landing_df.append(landing_row)
    return rank_headers, landing_headers, rank_df, landing_df

def structure(data, headers):
    df = pd.DataFrame(data, columns=headers)
    df.set_index('Keyword', inplace=True)
    return df

try:
    sdate = raw_input('Please Enter Start Date (YYYYMMDD): ')
    edate = raw_input('Please Enter End Date (YYYYMMDD): ')
    clientDF = clientProject()
    for i in clientDF.index:
        pName = clientDF['project_name'][i]
        projectId = clientDF['project_id'][i]
        projectUrl = clientDF['url'][i]
        jsonFile = ##fp_here## + pName +'.json'
        ofilePath = os.path.normpath(jsonFile)
        query = queryBuilder(sDate,eDate,projectId,projectUrl,majorKey())
        response = sendQuery(query)
        outputResponse(ofilePath, response)
        rank_headers, landing_headers, rank_df, landing_df = parseThe(jsonFile)
        df = structure(rank_df, rank_headers)
        df2 = structure(landing_df, landing_headers)
        rankO = pName + '_rank.csv'
        landingO = pName + '_landing_page.csv'
        df.to_csv(rankO, encoding='utf-8')
        df2.to_csv(landingO, encoding='utf-8')
except Exception as e:
    print e
