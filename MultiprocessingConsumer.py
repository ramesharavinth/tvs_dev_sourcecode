#!/usr/bin/env python
"""
Create multiple RabbitMQ connections from a single thread, using Pika and multiprocessing.Pool.
Based on tutorial 2 (http://www.rabbitmq.com/tutorials/tutorial-two-python.html).
"""
import functools
import json, pymysql
import multiprocessing
import time
import mysql.connector as mysql1

import pika

import logging
import pika
from pymysql import Error
import os
import datetime


def info(title):
    if hasattr(os, 'getppid'):  # only available on Unix
        print('parent process:', os.getppid())
    print('process id:', os.getpid())


def callback(ch, method, properties, body):
    print(" [x] %r received %r" % (
        multiprocessing.current_process(),
        body,
    ))
    time.sleep(body.count('.'))
    # print " [x] Done"
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_message(chan, method_frame, _header_frame, body, userdata=None):
    """Called when a message is received. Log message and ack it."""
    print('test')
    data = json.loads(body)
    #info('function f')
    #print(data)
    print('Message Id:' + str(data['id']) + ' End Time: ' + str(b))
    #sql = "INSERT INTO events(id,shift,station,duration,time,reason,updateby,updatedatetime,status,mac,imei,lat,lng,location) values(%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    sql = "INSERT INTO events(shift,station,duration,time,reason,updateby,updatedatetime,status,mac,imei,lat,lng,location) values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (data['shift'], data['station'], data['duration'], data['time'],
           data['reason'], data['updateby'], data['updatedatetime'],
           data['status'], data['mac'], data['imei'], data['lat'], data['lng'],
           data['location'])

    try:
        cur.execute(sql, val)
    except Error as e:
        print('Error:', e)
    conn.commit()

    b = datetime.datetime.now().replace(microsecond=0)
    print('Message Id:' + str(data['id']) + ' End Time: ' + str(b))

    chan.basic_ack(delivery_tag=method_frame.delivery_tag)


def consume():
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()

    channel.queue_bind(
        queue='tvsEventsQueue', exchange='amq.topic', routing_key='tvsEvent')
    channel.basic_qos(prefetch_count=1)
    on_message_callback = functools.partial(
        on_message, userdata='on_message_userdata')
    channel.basic_consume(
        on_message_callback,
        'tvsEventsQueue',
    )
    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    # conn = mysql1.connect(
    #     host='mysql6001.site4now.net',
    #     port=3306,
    #     user='a2c1fa_uat',
    #     passwd='bixware123',
    #     db='db_a2c1fa_uat')

    conn = mysql1.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='12345',
        db='freemandb')
    print('connected')
    print(multiprocessing.cpu_count())
    #Connect with mySql DB
    cur = conn.cursor()

    workers = 5
    pool = multiprocessing.Pool(processes=workers)
    for i in range(0, workers):
        pool.apply_async(consume)

    # Stay alive
    try:
        while True:
            continue
    except KeyboardInterrupt:
        print(' [*] Exiting...')
        pool.terminate()
        pool.join()
