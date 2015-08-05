package com.cdiscount.kafka;

import com.cdiscount.kafka.gateway.BrokerGateway;
import com.cdiscount.kafka.gateway.ConsumerGateway;
import com.cdiscount.kafka.gateway.ConsumerTopicGateway;
import com.cdiscount.kafka.gateway.TopicGateway;
import com.cdiscount.kafka.zookeeper.ZookeeperException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class Main {
    public static void main (String[] args) throws InterruptedException {
        //Example with one existing topic
//        try {
//            while(true) {
//
//                KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi("localhost:2181");
//
//                List<ConsumerGateway> consumers = kafkaGatewayApi.getConsumers();
//                ConsumerTopicGateway consumerTopic = consumers.get(0).consumerTopicsGateway.get(0);
//
//                System.out.println("LogSize : " + consumerTopic.logSize);
//                System.out.println("Lag : " + consumerTopic.lag);
//                System.out.println("Offset : " + consumerTopic.offset);
//                System.out.println("-----------------------------------------------");
//
//                Thread.sleep(5000);
//            }
//        } catch (ZookeeperException e) {
//            e.printStackTrace();
//        }

        try {
            KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi("localhost:2181");
            List<BrokerGateway> brokers = kafkaGatewayApi.getBrokers();
            List<TopicGateway> topics = kafkaGatewayApi.getTopics();
            List<ConsumerGateway> consumers = kafkaGatewayApi.getConsumers();
            //ConsumerTopicGateway consumerTopic = consumers.get(0).consumerTopicsGateway.get(0);

            BrokerGateway broker = brokers.get(0);
            System.out.println("Broker 1 : host : " + broker.host + ", id: " + broker.id + ", port: " + broker.port);
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
    }
}
