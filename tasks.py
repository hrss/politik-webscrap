# -*- coding: utf-8 -*-
"""
Created on Sun Oct  7 19:28:15 2018

@author: Lwz
"""

from celery import Celery
from celery.schedules import crontab

app = Celery('tasks', broker='amqp://guest:guest@ec2-54-149-173-164.us-west-2.compute.amazonaws.com:5672')

@app.task
def my_task(text='test'):
    print("foi",text)
    """ celery -A tasks worker --loglevel=info"""
   
@app.task
def test(arg):
    print(arg)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    sender.add_periodic_task(10.0, test.s('hello'), name='add every 10')

    # Calls test('world') every 30 seconds
    sender.add_periodic_task(30.0, test.s('world'), expires=10)

    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(hour=7, minute=30, day_of_week=1),
        test.s('Happy Mondays!'),
    )

