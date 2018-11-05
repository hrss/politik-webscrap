# -*- coding: utf-8 -*-
"""
Created on Sun Sep  9 02:29:18 2018

@author: Lwz
"""
import json
import requests
from unidecode import unidecode

def decode_names(politic_json_dict):
    for i in politic_json_dict["dados"]:
        i["nome"] = unidecode(i["nome"])

def json_print_file(politic_json_dict={}):
    
    with open("politicians.json",'w') as f:
        json.dump(politic_json_dict, f)

def webscrap_politicos():
    header = {'Content-Type': 'application/json' }
    
    api_url = "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome"
    n_api_url = ''
    
    response = requests.get(api_url, headers=header)
    
    politicians = { "dados" : []}
    
    '''if response.status_code == 200:'''
    while ( response.status_code == 200 and ( n_api_url != api_url )) :
        #print(json.loads(response.content.decode('utf-8')))
        
        json_objt = json.loads(response.content.decode('utf-8'))
        
        for x in json_objt["dados"] : politicians["dados"].append(x)
        
        print(api_url)
        api_url = n_api_url
        
        for i in json_objt["links"]:
            if i["rel"] == "next":
                n_api_url = i["href"]
                response = requests.get(n_api_url, headers=header)
        
    if(response.status_code != 200):
        print("Page Resquested Error:", response.status_code, "At url:" , api_url)
        raise Exception
    else:
        for i in politicians["dados"]:
            i["nome"] = unidecode(i["nome"])
        print(json.dumps(politicians,indent=2))

if __name__ == "__main__":
    webscrap_politicos()
    
    