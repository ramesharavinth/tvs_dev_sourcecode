import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
"""Basic message consumer example"""
import functools
import logging
import pika
import json, pymysql, time
from pymysql import Error

from multiprocessing import Process

class AppServerSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "ConsumerService"
    _svc_display_name_ = "Consumer Service"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def on_message(self,
                   chan,
                   method_frame,
                   _header_frame,
                   body,
                   conn, 
                   cur,
                   userdata=None):
        """Called when a message is received. Log message and ack it."""
        # LOGGER.info('Userdata: %s Message body: %s', userdata, body)

        data = json.loads(body)
        print("Id: %r" % data['id'])
        print("Name: %r" % data['name'])

        sql = "INSERT INTO pet (name,owner,species,sex,birth,death) VALUES(%s, %s, %s, %s, %s, %s)"
        val = (data['name'], 'Diane1', 'hamster1', 'f', '1999-03-30',
               '1999-03-30')

        try:
            cur.execute(sql, val)
        except Error as e:
            print('Error:', e)

        conn.commit()

        cur.close()
        conn.close()

        chan.basic_ack(delivery_tag=method_frame.delivery_tag)

    def main(self):
        """Main method."""
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='12345',
            db='freemanDB')

        #Connect with mySql DB
        cur = conn.cursor()

        credentials = pika.PlainCredentials('symc', 'symcrabbit')
        parameters = pika.ConnectionParameters('mqtt-perf.symctest.com', 5672,
                                               '/', credentials)
        connection = pika.BlockingConnection(parameters)

        channel = connection.channel()
        channel.queue_bind(
            queue='symcSubscriptionQueue',
            exchange='amq.topic',
            routing_key='symcSubscription')
        channel.basic_qos(prefetch_count=1)

        on_message_callback = functools.partial(
            self.on_message, userdata='on_message_userdata')
        channel.basic_consume(on_message_callback, 'symcSubscriptionQueue')

        try:
            start = time.time()

            channel.start_consuming()

            end = time.time()
            print(end - start)
        except KeyboardInterrupt:
            channel.stop_consuming()

        connection.close()

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppServerSvc)