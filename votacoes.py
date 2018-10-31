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
    
    with open("arqeuivovotacoes.json",'w') as f:
        json.dump(politic_json_dict, f)

def webscrap_propositions():
    header = {'Content-Type': 'application/json' }
    
    #api_url = "https://dadosabertos.camara.leg.br/api/v2/propositions?ordem=ASC&ordenarPor=id"
    #na requisicao da swagger temos siglaTipo = PL,PEC idSituacao = 924 Pronta para Pauta
    api_url = "https://dadosabertos.camara.leg.br/api/v2/proposicoes?siglaTipo=PEC%2CPL&idSituacao=924&ordem=ASC&ordenarPor=id"
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
    
    #We want just the propositions which changed the state so we can see their votes
    
    
        
    for i in propositions["dados"]:
        i["ementa"] = unidecode(i["ementa"])
    
    propositions_with_n_votes = {}
    propositions_with_n_votes["dados"] = []
    
    #we get the ids of the propositions wich we have at our db
    url = "https://c6d68e29.ngrok.io/api/non_voted"
    
    response = requests.get(url, headers=header)
    
    if(response.status_code != 200):
        print("Page Resquested Error:", response.status_code, "At url:" , api_url)
        raise
    
    lista_de_id = json.loads(response.content.decode('utf-8')); #simuladao de uma lista de ids de proposicoes ja no bd a ser recebida do endpoint
    
    #we keep just the propositions with changed state to look at the last voting section
    
    for i in propositions["dados"]:
        if str(i["id"]) not in lista_de_id:
            propositions_with_n_votes["dados"].append(i)
            break
            
    del propositions
            
    #print(json.dumps(propositions_with_n_votes,indent=2))
    
    #print(type(propositions["dados"][-1]["id"]))
    
    for i in propositions_with_n_votes["dados"]:
        #For each proposition we check the list of vote sections
        propo_id = str(2170090)
        api_url = "https://dadosabertos.camara.leg.br/api/v2/proposicoes/"+ propo_id + "/votacoes";#str(2170090) +"/votacoes"
        
        response = requests.get(api_url, headers=header)
        if(response.status_code != 200):
            print("Page Resquested Error:", response.status_code, "At url:" , api_url)
            raise
        
        json_objt = json.loads(response.content.decode('utf-8'));#Assuming here we wont have any pages to iter
        
        last_vote_id = str(json_objt["dados"][-1]["id"]);#this is the id of the last vote section
            
        #Now that we have the id of the last vote section we can create the json of the votes
        api_url = "https://dadosabertos.camara.leg.br/api/v2/votacoes/" + last_vote_id + "/votos"
        response = requests.get(api_url, headers=header)
        n_api_url = ''
        vote = {}; vote["prop_id"] = propo_id ;vote["dados"] = []
        
        while( response.status_code == 200 and ( n_api_url != api_url )):
            vote_objt = json.loads(response.content.decode('utf-8'))
            
            for x in vote_objt["dados"] : vote["dados"].append(x)
            api_url = n_api_url
            
            for i in vote_objt["links"]:
                if i["rel"] == "next":
                    n_api_url = i["href"]
                    response = requests.get(n_api_url, headers=header)
                
        if(response.status_code != 200):
            print("Page Resquested Error:", response.status_code, "At url:" , api_url)
            raise
        
        text = json.dumps(vote)
        
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('ec2-54-149-173-164.us-west-2.compute.amazonaws.com',
                                               5672,
                                               '/',
                                               credentials)
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        channel.queue_declare(queue='politik_votes')
        
        channel.basic_publish(exchange='', routing_key='politik_votes', body=text)
                
        connection.close()
        
        break
        

if __name__ == "__main__":
    webscrap_propositions()
    
