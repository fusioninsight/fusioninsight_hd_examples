#!/usr/bin/env bash
# cd /opt/client 进入实际客户端安装目录
# source bigdata_env 导入环境变量
# kinit 使用具有KafkaAdmin权限的用户登录
# cd ./Kafka/kafka/bin/ 进入Kafka脚本目录

# 查询当前已有的topic
./kafka-topics.sh --list --zookeeper 187.7.67.6:24002,187.7.67.88:24002,187.7.66.109:24002/kafka

# 如果没有test-topic，则创建。
# partition数量估算：MAX（SumP（业务写入总吞吐量)/P(单partition写入性能),SumC（业务读取总吞吐量/C（单partition读取性能））。
# 如果要保证消息按顺序被消费，就只建一个partition。
./kafka-topics.sh --create --zookeeper 187.7.67.6:24002,187.7.67.88:24002,187.7.66.109:24002/kafka --partitions 3 --replication-factor 2 --topic test-topic

# 给producer赋予向topic中生产数据的权限
./kafka-acls.sh --authorizer-properties zookeeper.connect=187.7.67.6:24002,187.7.67.88:24002,187.7.66.109:24002/kafka --add --allow-principal User:TestUser --producer --topic test-topic

# 给consumer赋予从topic中消费数据的权限
./kafka-acls.sh --authorizer-properties zookeeper.connect=187.7.67.6:24002,187.7.67.88:24002,187.7.66.109:24002/kafka --add --allow-principal User:TestUser --consumer --topic test-topic --group test-group1
