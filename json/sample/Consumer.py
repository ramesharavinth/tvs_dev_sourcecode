#!/usr/bin/python
# -*- coding: utf-8 -*-

import pika
import json

print('pika version: %s' % pika.__version__)

credentials = pika.PlainCredentials('symc', 'symcrabbit')
parameters = pika.ConnectionParameters('mqtt-perf.symctest.com', 5672, '/',
                                       credentials)
connection = pika.BlockingConnection(parameters)

main_channel = connection.channel()
consumer_channel = connection.channel()
bind_channel = connection.channel()

#main_channel.exchange_declare(exchange='amq.topic',       exchange_type='topic')
# main_channel.exchange_declare(exchange='com.micex.lasttrades', exchange_type='direct')

queue = main_channel.queue_declare('', exclusive=True).method.queue
queue_tickers = main_channel.queue_declare('', exclusive=True).method.queue

main_channel.queue_bind(
    exchange='amq.topic', queue=queue, routing_key='symcSubscription')


def hello():
    print('Hello world')


connection.add_timeout(5, hello)


def callback(ch, method, properties, body):
    body = json.loads(body)['symcSubscription']

    ticker = None
    if 'ticker' in body['data']['params']['condition']:
        ticker = body['data']['params']['condition']['ticker']
    if not ticker: return

    print('got ticker %s, gonna bind it...' % ticker)
    bind_channel.queue_bind(
        exchange='amq.topic',
        queue='symcSubscriptionQueue',
        routing_key='symcSubscription')
    print('ticker %s binded ok' % ticker)


import logging
logging.basicConfig(level=logging.INFO)

consumer_channel.basic_consume(callback, queue)

try:
    consumer_channel.start_consuming()
finally:
    connection.close()
