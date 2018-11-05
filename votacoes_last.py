# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 19:25:29 2018

@author: Lwz
"""
import requests
import json
import pika
from unidecode import unidecode

def webscrap_votacoes_last():
    
    url = "https://dadosabertos.camara.leg.br/api/v2/votacoes?ordem=ASC&ordenarPor=id"
    header = {'Content-Type': 'application/json' }
    
    n_url = ''
    
    response = requests.get(url, headers=header)
    
    votacoes_last = { "dados" : []}
    
    while( response.status_code == 200 and ( n_url != url )):
        json_objt = json.loads(response.content.decode('utf-8'))
        
        for x in json_objt["dados"] : votacoes_last["dados"].append(x)
        
        url = n_url
        
        for i in json_objt["links"]:
            if i["rel"] == "next":
                n_url = i["href"]
                response = requests.get(n_url, headers=header)
        
                
    if(response.status_code != 200):
        print("Page Resquested Error:", response.status_code, "At url:" , url)
        raise
    else:
        for i in votacoes_last["dados"]:
            '''
            i["titulo"] = unidecode(i["titulo"])
            i["processoVotacao"] = unidecode(i["processoVotacao"])
            i["aprovada"] = unidecode(i["aprovada"])
            i["proposicao"]["ementa"] = unidecode(i["proposicao"]["ementa"])
            '''
            
            #we ignore status REQUERIMENTOS
            if not (i["titulo"].startswith("REQUERIMENTO")) :
                
                url = "https://dadosabertos.camara.leg.br/api/v2/votacoes/"+str(i["id"])+"/votos?itens=513" 
                
                n_url = ''
        
                response = requests.get(url, headers=header)
                
                vote = {}; vote["vote_id"]=str(i["id"]);vote["prop_id"] = str(i["proposicao"]["id"]);vote["dados"] = []
                #print(vote["prop_id"]);#debug
                #print(i);#debug
                while( response.status_code == 200 and ( n_url != url )):
                    json_objt = json.loads(response.content.decode('utf-8'))
                    
                    for x in json_objt["dados"] : vote["dados"].append(x)
                    
                    url = n_url
                    
                    for i in json_objt["links"]:
                        if i["rel"] == "next":
                            n_url = i["href"]
                            response = requests.get(n_url, headers=header)
                #print(json.dumps(vote,indent=2));#debug
               
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

if __name__ == "__main__":
    webscrap_votacoes_last()