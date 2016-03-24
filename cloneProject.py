# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 07:06:54 2016
@author: ktarvin

semRush charges 10x the price for competitor keyword research for tracked clients.
By cloning your client project for a competitor, the cost of competitive reporting
can be reduced. (the impact of savings increase if you do weekly reporting rather
than monthly reporting)

The function of this script is to clone client keyword tracking projects to
capture that cost savings. It assumes Project related data is already collected
and stored in a csv. Pandas is utilized to leverage that client data to itterate
over multiple projects. This version also assumes projects are already created.

schema of client data csv: 
[
project_name,
project_id,
url_to_track,
country,
country_id, #this one will need you to access their country ID API
competitors_project_id,
competitor_project_name,
competitor_url
]
"""

import requests, json, io, time
import pandas as pd
import ijson

def majorKey(): 
    '''Fetches API key for semRush'''
    with open(r'insert_file_path_here', 'r') as f:
        ApiKey = f.read()
        return ApiKey

def queryBuilder(projectId,ApiKey):
    '''
    Builds the API enpoint URL
        Args:
            projectId >>> The semRush project id for the keyword project
            ApiKey >>> semRush Api key
            query >>> the url of the endpoint where you make a request
    '''
    pId = unicode(projectId)
    apiKey = u'?key='+unicode(ApiKey)
    api = u'http://api.semrush.com/management/v1/projects/'+pId+'/keywords'
    query = api+apiKey
    return query

def putQuery(query, package):
    '''
    Makes the API request
        args: 
            query >>> the endpoint url for semRush's API
            package >>> Data formatted as a dictionary being sent to API
            response >>> JSON formatted API response
    '''
    r = requests.put(query, json = package)
    response = r.json()
    return response

def outputResponse(o, response):
    '''
    Writes the API response to file.
        args: 
            o >>> file name
            response >>> API response in JSON format
    '''
    with io.open(o, 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(response, ensure_ascii=False)))

def cloneProject(client_name):
    '''
    Takes project_get API response of client's keywords and clones 
    the project for a competitor. Function assumes project_get responses are
    stored in same directory.
        args:
            client_name >>> name of client as string
            package >>> dictionary of keywords with tags for a given project
    '''
    fn = client_name + '.json'    
    keyword_list = []
    with open(fn, 'r') as f:
        kw = ijson.items(f, 'keywords')
        obj = list(kw)
        for kw in obj:
            for i in enumerate(x for x in kw):
                keyword_dict = i[1]
                del keyword_dict['timestamp']
                keyword_list.append(keyword_dict)
#                package = json.dumps({u'keywords':keyword_list}, ensure_ascii=False)
                package = {u'keywords':keyword_list}
    return package

 
try:
    df = pd.read_csv('client_data_table_goes_here')
    for i in df.index:
        client_name = df['project_name'][i]
        client = df['competitor_project_name'][i]
        projectId = df['project_id'][i]
        package = cloneProject(client)
        query = queryBuilder(projectId,majorKey())
        response = putQuery(query, package)
        print client_name, '\n', response
        o = client_name + '.json'
        outputResponse(o, response)
        time.sleep(1) #prevents exceeding rate limit
except Exception as e:
    print e
