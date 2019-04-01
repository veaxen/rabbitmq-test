#!/usr/bin/env python
# coding=utf-8

import getopt
import sys
from log import log
from consumer import Consumer
from producer import Producer
import time

g_ip = "192.168.10.100"
g_exchange = "to_consumer"
g_user = "test"
g_password = "test"

g_test_secs = 1 * 60
g_sleep_secs = 1

g_queue_name = ["test1_", "entry_", "test2_", "test3_",
                "test4_", "test5_", "test6_", "test7_", "test8_"]


# 测试长连接
def test_keep_alive():
    # 创建生产者
    p = Producer(ip=g_ip, user=g_user, password=g_password)
    p.producer_declare()
    p.create_exchange(g_exchange, "topic")

    # 创建消费者
    c = Consumer(ip=g_ip, user=g_user, password=g_password)
    c.start_consumer(g_exchange, "test1", "test1")

    time.sleep(5)   # 等5秒让队列准备就绪

    # 保持不发送任何消息
    log.info("[test_keep_alive] start sending nothing test ...")
    secs = 0
    while secs < g_test_secs:
        time.sleep(5)
        secs += 5

    try:
        # 发送一次消息检查连接可用性
        log.info("[test_keep_alive] test connection alive???")
        p.publish(g_exchange, "docx2pdf", '{"msg":"this is a test!"}')
        log.info("[test_keep_alive] connection alive!!!")
        log.info("[test_keep_alive] start sending msg test ...")
        secs = 0
        while secs < g_test_secs:
            time.sleep(1)
            p.publish(g_exchange, "test1", '{"msg":"this is a test!"}')
            secs += 1
    except Exception as err:
        log.error("[test_keep_alive] error: " + str(err))
        log.error("exit [test_keep_alive]")
    finally:
        p.publish(g_exchange, "test1", "quit")
        p.close()  # 关闭生产者连接
        c.join()   # 等待消费线程结束


def test_qps():
    # 创建生产者
    p = Producer(ip=g_ip, user=g_user, password=g_password)
    p.producer_declare()
    p.create_exchange(g_exchange, "topic")

    # 创建消费者
    consumers = []

    for queue_name in g_queue_name:
        for i in range(0, 3):
            consumers.append(Consumer(ip=g_ip, user=g_user, password=g_password))
            consumers[len(consumers)-1].start_consumer(g_exchange, queue_name + str(i), queue_name + str(i))

    time.sleep(10)  # 等待10S, 让消费者绑定完成
    log.info("[test_qps] starting ...")

    try:
        target_time = g_test_secs
        start = time.time()
        stop = False
        while not stop:
            for queue_name in g_queue_name:
                for i in range(0, 3):
                    time.sleep(g_sleep_secs)
                    p.publish(g_exchange, queue_name + str(i), '{"msg":"this is a test!"}')
                    curr = time.time()
                    if (curr-start) >= target_time:
                        stop = True
                        break
                if stop:
                    break

    except Exception as err:
        log.error("[test_qps] error: " + str(err))
    finally:
        for queue_name in g_queue_name:
            for i in range(0, 3):
                p.publish(g_exchange, queue_name + str(i), "quit")
        p.close()

        recev = 0
        last_time = 0.0
        for c in consumers:
            c.join()
            recev += c.number_of_msg()
            if c.stop_consume_time() > last_time:
                last_time = c.stop_consume_time()

        log.info("[test_qps] %d msg have been sent, start at %f" %
                     (p.number_of_msg(), p.start_publish_time()))
        log.info("[test_qps] %d msg have been received, end at %f" %
                 (recev, last_time))
        log.info("[test_qps] QPS: %f" % (recev / (last_time - p.start_publish_time())))


def usage():
    print(
            """
            Usage:sys.args[0] [option]
            -h or --help：显示帮助信息
            -p or --project：测试项目
            -t or --time：持续时间（分钟）
            -q or --qps：每秒生产消息数量
            """
    )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        usage()
        sys.exit()

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:t:q:",
                                   ["help", "project=", "time=", "qps="])
    except getopt.GetoptError:
        print("argv error,please input")

    project = None
    for cmd, arg in opts:
        if cmd in ("-h", "--help"):
            usage()
            sys.exit()
        elif cmd in ("-p", "--project"):
            project = arg
        elif cmd in ("-t", "--time"):
            g_test_secs = g_test_secs * int(arg)
        elif cmd in ("-q", "--qps"):
            g_sleep_secs = 1.0 / int(arg)

    if project == "alive":
        log.info("/******************************** Test keep alive ********************************/")
        test_keep_alive()
        log.info("/**************************** Test keep alive finished ***************************/\n")
    elif project == "qps":
        log.info("/*********************************** Test QPS ************************************/")
        test_qps()
        log.info("/******************************** Test QPS finished ******************************/\n")
