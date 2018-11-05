# -*- coding: utf-8 -*-
"""
@author: Lwz
"""
import pika
import json
import requests
from unidecode import unidecode

def decode_names(propositions):
    for i in propositions["dados"]:
        i["ementa"] = unidecode(i["ementa"])

def json_print_file(politic_json_dict={}):
    
    with open("politicians.json",'w') as f:
        json.dump(politic_json_dict, f)

def webscrap_propositions():
    header = {'Content-Type': 'application/json' }
    
    api_url = "https://dadosabertos.camara.leg.br/api/v2/propositions?ordem=ASC&ordenarPor=id"
    #api_url = "https://dadosabertos.camara.leg.br/api/v2/proposicoes?siglaTipo=PEC%2CPL&ordem=ASC&ordenarPor=id"
    
    #na requisicao da swagger temos siglaTipo = PL,PEC idSituacao = 924 Pronta para Pauta
    #api_url = "https://dadosabertos.camara.leg.br/api/v2/proposicoes?siglaTipo=PEC%2CPL&idSituacao=924&ordem=ASC&ordenarPor=id"
    n_api_url = ''
    
    response = requests.get(api_url, headers=header)
    
    propositions = { "dados" : []}
    
    while( response.status_code == 200 and ( n_api_url != api_url )):
        json_objt = json.loads(response.content.decode('utf-8'))
        
        for x in json_objt["dados"] : propositions["dados"].append(x)
        
        api_url = n_api_url
        
        for i in json_objt["links"]:
            if i["rel"] == "next":
                n_api_url = i["href"]
                response = requests.get(n_api_url, headers=header)
        
                
    if(response.status_code != 200):
        print("Page Resquested Error:", response.status_code, "At url:" , api_url)
        raise
    else:
        for i in propositions["dados"]:
            i["ementa"] = unidecode(i["ementa"])
            
        text = json.dumps(propositions)
        
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('ec2-54-149-173-164.us-west-2.compute.amazonaws.com',
                                               5672,
                                               '/',
                                               credentials)
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        channel.queue_declare(queue='politik_law_projects')
        
        channel.basic_publish(exchange='', routing_key='politik_law_projects', body=text)
                
        connection.close()
    

if __name__ == "__main__":
    webscrap_propositions()