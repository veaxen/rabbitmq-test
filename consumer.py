#!/usr/bin/env python
# coding=utf-8

from log import log
import threading
import thread
import pika
import time


class Consumer(threading.Thread):

    def __init__(self, ip, user, password):
        threading.Thread.__init__(self)
        self.__ip = ip
        self.__user = user
        self.__password = password
        self.__channel = None
        self.__connection = None
        self.__exchange_Name = None
        self.__queue_name = None
        self.__routing_key = None
        self.__callback = None
        self.__number_of_msg = 0
        self.__first_msg_time = 0.0
        self.__quit_time = 0.0
        self.__stop_consuming = False

    def __connect(self):
        try:
            credentials = pika.PlainCredentials(self.__user, self.__password)
            conn_params = pika.ConnectionParameters(self.__ip, credentials=credentials, virtual_host="/")

            self.__connection = pika.BlockingConnection(conn_params)
            self.__channel = self.__connection.channel()
        except Exception as err:
            raise Exception("Consumer __connect error: " + str(err))

    def __consumer_declare(self):
        try:
            if self.__channel is None:
                raise Exception("channel can not be None.")

            # todo 暂时只支持topic模式
            self.__channel.exchange_declare(exchange=self.__exchange_Name, exchange_type="topic", durable=True)

            self.__channel.queue_declare(self.__queue_name, exclusive=False)
            self.__channel.queue_bind(exchange=self.__exchange_Name, queue=self.__queue_name,
                                      routing_key=self.__routing_key)
        except Exception as err:
            raise Exception("Consumer __consumer_declare error:" + str(err))

    def ack_msg(self, delivery_tag=0):
        try:
            self.__channel.basic_ack(delivery_tag=delivery_tag)
        except Exception as err:
            log.error("Consumer: %s ack_msg error: %s" % (self.__queue_name, str(err)))

    def start_consumer(self, exchange_name=None, routing_key=None, queue_name=None):
        try:
            Consumer.__check_consume_params(exchange_name, queue_name, routing_key)
            self.__exchange_Name = exchange_name
            self.__queue_name = queue_name
            self.__routing_key = routing_key
            self.__callback = self.consumer_callback

            # 启动新线程进行消费
            self.start()
        except Exception as err:
            raise Exception("Consumer start_consumer error: " + str(err))

    def run(self):
        while not self.__stop_consuming:
            try:
                self.__connect()
                self.__consumer_declare()
                self.__channel.basic_qos(prefetch_count=1)
                self.__channel.basic_consume(self.__on_callback, queue=self.__queue_name, no_ack=False)

                # 开始消费
                self.__stop_consuming = False
                self.__channel.start_consuming()
            except Exception as err:
                log.error("Consumer run error: " + str(err))
                self.__close()

    def __on_callback(self, channel, method, properties, body):
        thread.start_new_thread(self.__callback, (channel, method, properties, body))

    @staticmethod
    def __check_consume_params(exchange_name, queue_name, routing_key):
        if exchange_name is None:
            raise Exception("exchangeName is none")

        if queue_name is None:
            raise Exception("queueName is none")

        if routing_key is None:
            raise Exception("routingkey is none")

    def __close(self):
        self.__close_impl(self.__connection, self.__channel)

        self.__connection = None
        self.__channel = None

    @staticmethod
    def __close_impl(connection, channel):
        try:
            if channel is not None:
                channel.stop_consuming()

            if connection is not None:
                connection.close()
        except:
            pass

    def consumer_callback(self, channel, method, properties, body):
        self.__number_of_msg += 1
        if self.__number_of_msg == 1:
            self.__first_msg_time = time.time()
        self.ack_msg(method.delivery_tag)
        if str(body) == "quit":
            self.__quit_time = time.time()
            log.info("Consumer: %s quit at %f, Number of Msg: %d" %
                         (self.__queue_name, self.__quit_time, self.__number_of_msg))
            self.__channel.basic_cancel(consumer_tag=self.__queue_name)
            self.__channel.stop_consuming()
            self.__stop_consuming = True

    def stop_consume_time(self):
        return self.__quit_time

    def number_of_msg(self):
        return self.__number_of_msg
