# -*- coding: utf-8 -*-
"""
Created on Sun Sep  9 02:29:18 2018

@author: Lwz
"""
import json
import requests
#from unidecode import unidecode

'''def debug_test(politic_json_dict):
    for i in politic_json_dict["dados"]:
        i["nome"] = unidecode(i["nome"])'''

def json_print_file(politic_json_dict={}):
    
    with open("politicians.json",'w') as f:
        json.dump(politic_json_dict, f)

def politic():
    header = {'Content-Type': 'application/json' }
    
    api_url = "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome"
    
    response = requests.get(api_url, headers=header)
    
    politicians = { "dados" : []}
    
    if response.status_code == 200:
        #print(json.loads(response.content.decode('utf-8')))
        
        for i in range(1,36):
            api_url = 'https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome&pagina='+str(i)+'&itens=15'
            response = requests.get(api_url, headers=header)
            data = json.loads(response.content.decode('utf-8'))
            #politicians.update(data)
            for x in data["dados"] : politicians["dados"].append(x)
        #debug_test(politicians)
        json_print_file(politicians)
    else:
        print("Page Resquested Error:", response.status_code)
        raise
if __name__ == "__main__":
    politic()