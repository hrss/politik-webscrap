# -*- coding: utf-8 -*-
"""
@author: Lwz
"""

import pika

f = open('politicians.json','r')
text = f.read()
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('ec2-54-149-173-164.us-west-2.compute.amazonaws.com',
                                       5672,
                                       '/',
                                       credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='politik_test')

channel.basic_publish(exchange='', routing_key='politik_politicians', body=text)

print(" [x] Sent {0}".format(text))

connection.close()