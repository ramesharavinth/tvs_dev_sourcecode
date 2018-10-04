#!/usr/bin/env python
import pika
import sys, json, datetime
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/',
                                       credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# channel.exchange_declare(exchange='amqp.topic',
#                          exchange_type='topic')
for i in range(0, 100):
    message = json.dumps({
        "id": i + 1,
        "shift": 1,
        "station": 1,
        "duration": "2016-04-08 11:43:36.309721",
        "time": "2016-04-08 11:43:36.309721",
        "reason": "Test Reason",
        "updateby": "Ramesh",
        "updatedatetime": "2016-04-08 11:43:36.309721",
        "status": "Completed",
        "mac": "Test",
        "imei": 1,
        "lat": 12.1,
        "lng": 11.1,
        "location": "Bangalore"
    })

    channel.basic_publish(
        exchange='amq.topic', routing_key='tvsEvents', body=message)
    print(" [x] Sent %r" % message)
connection.close()