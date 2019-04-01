#!/usr/bin/env python
# coding=utf-8

import logging


class Logger(object):

    def __init__(self):
        self.__logger = logging.getLogger("RabbitMQ_HA_Test")
        self.__logger.setLevel(level=logging.INFO)
        stream = logging.StreamHandler()
        stream.setLevel(logging.INFO)
        self.__logger.addHandler(stream)

    def debug(self, msg):
        self.__logger.debug(msg)

    def info(self, msg):
        self.__logger.info(msg)

    def warning(self, msg):
        self.__logger.warning(msg)

    def error(self, msg):
        self.__logger.error(msg)


log = Logger()
