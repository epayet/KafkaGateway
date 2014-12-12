package com.cdiscount.kafka;

import com.cdiscount.kafka.info.BrokerInfo;
import com.cdiscount.kafka.info.ConsumerInfo;
import com.cdiscount.kafka.info.ConsumerTopicInfo;
import com.cdiscount.kafka.info.TopicInfo;
import com.cdiscount.kafka.utils.KafkaApiUtils;
import com.cdiscount.kafka.zookeeper.ZookeeperClient;
import com.cdiscount.kafka.zookeeper.ZookeeperException;
import kafka.javaapi.consumer.SimpleConsumer;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 * TODO : Get the other partitions
 */
public class KafkaInfoApi {
    private ZookeeperClient zookeeperClient;
    private KafkaApiUtils kafkaApiUtils;

    public KafkaInfoApi() {
    }

    public KafkaInfoApi(ZookeeperClient zookeeperClient, KafkaApiUtils kafkaApiUtils) {
        this.zookeeperClient = zookeeperClient;
        this.kafkaApiUtils = kafkaApiUtils;
    }

    public List<ConsumerInfo> getConsumersInfo() throws ZookeeperException {
        List<ConsumerInfo> consumersInfo = getConsumersInfoFromZookeeper();
        for (ConsumerInfo consumerInfo : consumersInfo) {
            for (ConsumerTopicInfo consumerTopicInfo : consumerInfo.topicsInfo) {
                consumerTopicInfo.logSize = getLogSize(consumerInfo.name, consumerTopicInfo.name);
                consumerTopicInfo.lag = consumerTopicInfo.logSize - consumerTopicInfo.offset;
            }
        }
        return consumersInfo;
    }

    public List<ConsumerInfo> getConsumersInfoFromZookeeper() throws ZookeeperException {
        List<String> consumersNames = zookeeperClient.getChildren("/consumers");
        return createConsumersInfo(consumersNames);
    }

    public long getLogSize(String consumerName, String topicName) throws ZookeeperException {
        BrokerInfo brokerInfo = getLeaderBroker(topicName);
        SimpleConsumer kafkaConsumer = kafkaApiUtils.createSimpleConsumer(brokerInfo.host, brokerInfo.port, topicName);
        return kafkaApiUtils.getLastOffset(kafkaConsumer, topicName, 0, kafkaApiUtils.getOffsetLatestTime(), consumerName);
    }

    public BrokerInfo getLeaderBroker(String topicName) throws ZookeeperException {
        List<TopicInfo> topicsInfo = getTopicsInfo();
        int leaderId = 0;
        for(TopicInfo topicInfo : topicsInfo) {
            if (topicInfo.name.equals(topicName)) {
                leaderId = topicInfo.leaderId;
            }
        }
        List<BrokerInfo> brokersInfo = getBrokersInfo();
        for (BrokerInfo brokerInfo : brokersInfo) {
            if (brokerInfo.id == leaderId) {
                return brokerInfo;
            }
        }
        return null;
    }

    public List<TopicInfo> getTopicsInfo() throws ZookeeperException {
        List<TopicInfo> topicsInfo = new ArrayList<TopicInfo>();
        List<String> topicNames = zookeeperClient.getChildren("/brokers/topics");
        for (String topicName : topicNames) {
            TopicInfo topicInfo = new TopicInfo();
            topicInfo.name = topicName;

            topicInfo.partitionsIds = new ArrayList<Integer>();
            List<String> partitions = zookeeperClient.getChildren("/brokers/topics/" + topicName + "/partitions");
            for (String partition : partitions) {
                topicInfo.partitionsIds.add(Integer.parseInt(partition));
            }

            JSONObject data = zookeeperClient.getJsonData("/brokers/topics/" + topicName + "/partitions/0/state");
            topicInfo.leaderId = data.getInt("leader");

            topicsInfo.add(topicInfo);
        }

        return topicsInfo;
    }

    public List<BrokerInfo> getBrokersInfo() throws ZookeeperException {
        List<BrokerInfo> brokersInfo = new ArrayList<BrokerInfo>();

        List<String> brokersIds = zookeeperClient.getChildren("/brokers/ids");
        for (String brokerId : brokersIds) {
            BrokerInfo brokerInfo = new BrokerInfo();
            brokerInfo.id = Integer.parseInt(brokerId);
            JSONObject data = zookeeperClient.getJsonData("/brokers/ids/" + brokerId);
            brokerInfo.host = data.getString("host");
            brokerInfo.port = data.getInt("port");
            brokersInfo.add(brokerInfo);
        }

        return brokersInfo;
    }

    private ArrayList<ConsumerInfo> createConsumersInfo(List<String> consumersNames) throws ZookeeperException {
        ArrayList<ConsumerInfo> consumersInfo = new ArrayList<ConsumerInfo>();
        for (String name : consumersNames) {
            ConsumerInfo consumerInfo = new ConsumerInfo();
            consumerInfo.name = name;
            consumerInfo.topicsInfo = createConsumerTopicsInfo(consumerInfo.name);
            consumersInfo.add(consumerInfo);
        }
        return consumersInfo;
    }

    private List<ConsumerTopicInfo> createConsumerTopicsInfo(String consumerName) throws ZookeeperException {
        List<ConsumerTopicInfo> consumerTopicsInfo = new ArrayList<ConsumerTopicInfo>();
        try {
            List<String> consumerTopics = zookeeperClient.getChildren("/consumers/" + consumerName + "/offsets");
            for (String topic : consumerTopics) {
                ConsumerTopicInfo consumerTopicInfo = new ConsumerTopicInfo();
                consumerTopicInfo.name = topic;
                consumerTopicInfo.offset = getConsumerTopicOffset(consumerName, topic);
                consumerTopicsInfo.add(consumerTopicInfo);
            }
        } catch (ZookeeperException zookeeperException) {

        }
        return consumerTopicsInfo;
    }

    private long getConsumerTopicOffset(String consumerName, String topic) throws ZookeeperException {
        String consumerTopicOffset = zookeeperClient.getData("/consumers/" + consumerName + "/offsets/" + topic + "/0");
        return Long.parseLong(consumerTopicOffset);
    }

    public void setKafkaApiUtils(KafkaApiUtils kafkaApiUtils) {
        this.kafkaApiUtils = kafkaApiUtils;
    }

    public void setZookeeperClient(ZookeeperClient zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }
}
