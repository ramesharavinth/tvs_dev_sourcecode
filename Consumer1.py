"""Basic message consumer example"""
import functools
import logging
import pika
import json, pymysql
from pymysql import Error
from multiprocessing import Process
import multiprocessing
import os
import mysql.connector as mysql1
import datetime


# def info(title):
#     if hasattr(os, 'getppid'):  # only available on Unix
#         print('parent process:', os.getppid())
#     print('process id:', os.getpid())


def on_message(chan, method_frame, _header_frame, body, userdata=None):
    """Called when a message is received. Log message and ack it."""
    data = json.loads(body)
    # info('function f')

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
    print('Message Id:'+ str(data['id']) + ' End Time: ' + str(b))

    chan.basic_ack(delivery_tag=method_frame.delivery_tag)


def main(cur):
    """Main method."""
    print('Main Called')
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()

    channel.queue_bind(
        queue='tvsEventsQueue', exchange='amq.topic', routing_key='tvsEvent')
    channel.basic_qos(prefetch_count=1)

    a = datetime.datetime.now().replace(microsecond=0)
    print('Start Time: ' + str(a))

    on_message_callback = functools.partial(
        on_message, userdata='on_message_userdata')
    
    channel.basic_consume(
        on_message_callback,
        'tvsEventsQueue',
    )
    try:
        print('started consuming')
        channel.start_consuming()
        print('finished consuming')

    except KeyboardInterrupt:
        channel.stop_consuming()

    connection.close()


if __name__ == '__main__':
    conn = mysql1.connect(
        host='mysql6001.site4now.net',
        port=3306,
        user='a2c1fa_uat',
        passwd='bixware123',
        db='db_a2c1fa_uat')
    print('connected')
    print(multiprocessing.cpu_count())
    #Connect with mySql DB
    cur = conn.cursor()

    processes = []
    for i in range(10):
        t = multiprocessing.Process(target=main(cur), args=(i, ))
        processes.append(t)
        t.start()

    for one_process in processes:
        one_process.join()

    print("Done!")

    # p1 = Process(target=main(cur))
    # p1.start()
    # print('Calling Main')
    # p1.join()

    print('committed')
    cur.close()
    conn.close()