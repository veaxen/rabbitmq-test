#!/usr/bin/env python
# coding=utf-8

from log import log
import time
import pika
from pika.exceptions import ConnectionClosed


class Producer:

    def __init__(self, ip, user, password):
        self.__ip = ip
        self.__user = user
        self.__password = password
        self.__channel = None
        self.__connection = None
        self.__reconnect = False
        self.__msg_number = 0
        self.__start_publish_time = 0.0

    def __connect(self):
        try:
            credentials = pika.PlainCredentials(self.__user, self.__password)
            conn_params = pika.ConnectionParameters(self.__ip, virtual_host="/", credentials=credentials)

            self.__connection = pika.BlockingConnection(conn_params)
            self.__channel = self.__connection.channel()
        except Exception as err:
            raise Exception("Producer __connect error: " + str(err))

    def producer_declare(self):
        try:
            self.__connect()
        except Exception as err:
            raise err

    def create_exchange(self, exchange_name, exchange_type):
        try:
            if exchange_name is None:
                raise Exception("exchange_name can not be None.")
            if exchange_type is None:
                exchange_type = "topic"

            # 定义为持久化exchange
            self.__channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        except Exception as err:
            raise Exception("Producer create_exchange error: " + str(err))

    def publish(self, exchange_name, routing_key, body):
        try:
            if exchange_name is None:
                raise Exception("Producer publish exchange_name is None")

            if routing_key is None:
                raise Exception("Producer publish routing_key is None")

            self.__channel.basic_publish(exchange=exchange_name, routing_key=routing_key,
                                         properties=pika.BasicProperties(content_type="application/octet-stream"),
                                         body=bytes(body))
            self.__msg_number += 1
            if self.__msg_number == 1:
                self.__start_publish_time = time.time()
        # 捕获ConnectionClosed异常，触发重连
        except ConnectionClosed as err:
            if not self.__reconnect:
                log.info("Producer connection closed, try to reconnect ...")
                self.__connect()
                self.__reconnect = True
                self.publish(exchange_name, routing_key, body)
                log.info("Producer reconnect success!")
            else:
                raise Exception("Producer publish error: " + str(err))
        else:
            self.__reconnect = False

    def close(self):
        Producer.__close(self.__connection, self.__channel)
        log.info("Producer quit, start at %f, Number of Msg: %d" % (self.__start_publish_time, self.__msg_number))

    @staticmethod
    def __close(connection, channel):
        try:
            if channel is not None:
                channel.stop_consuming()

            if connection is not None:
                connection.close()
        except:
            pass

    def start_publish_time(self):
        return self.__start_publish_time

    def number_of_msg(self):
        return self.__msg_number
