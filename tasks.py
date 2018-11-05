# -*- coding: utf-8 -*-
"""
Created on Sun Oct  7 19:28:15 2018

@author: Lwz
"""

from celery import Celery
from celery.schedules import crontab

import json
import requests
from unidecode import unidecode
import pika

app = Celery('tasks', broker='amqp://guest:guest@ec2-54-149-173-164.us-west-2.compute.amazonaws.com:5672')

@app.task
def webscrap_politicians():
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
        
        print(api_url,n_api_url)
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
        
        text = json.dumps(politicians)
        
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('ec2-54-149-173-164.us-west-2.compute.amazonaws.com',
                                               5672,
                                               '/',
                                               credentials)
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        channel.queue_declare(queue='politik_politicians')
        
        channel.basic_publish(exchange='', routing_key='politik_politicians', body=text)
        
        connection.close()
    

@app.task
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
        
        '''
        for i in json_objt["links"]:
            if i["rel"] == "next":
                n_api_url = i["href"]
                response = requests.get(n_api_url, headers=header)
        '''
                
    if(response.status_code != 200):
        print("Page Resquested Error:", response.status_code, "At url:" , api_url)
        raise Exception
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

@app.task
def webscrap_last_votes():
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

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    '''
    # Calls test('hello') every 10 seconds.
    sender.add_periodic_task(10.0, test.s('hello'), name='add every 10')
    # Calls test('world') every 30 seconds
    sender.add_periodic_task(30.0, test.s('world'), expires=10)

    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(hour=7, minute=30, day_of_week=1),
        test.s('Happy Mondays!'),
    )'''
    sender.add_periodic_task(
        360,
        webscrap_politicians.s(),
    )
    sender.add_periodic_task(
        300,
        webscrap_propositions.s(),
    )
    sender.add_periodic_task(
        60,
        webscrap_last_votes.s(),
    )
    

