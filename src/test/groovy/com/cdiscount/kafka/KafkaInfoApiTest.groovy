package com.cdiscount.kafka

import com.cdiscount.kafka.info.BrokerInfo
import com.cdiscount.kafka.info.ConsumerInfo
import com.cdiscount.kafka.info.ConsumerTopicInfo
import com.cdiscount.kafka.utils.KafkaApiUtils
import com.cdiscount.kafka.zookeeper.MockZookeeperClient
import org.json.JSONObject
import spock.lang.Specification

/**
 * Created by emmanuel_payet on 27/11/14.
 */
class KafkaInfoApiTest extends Specification {

    def kafkaUtils

    void setup() {
        kafkaUtils = new KafkaInfoApi(createSimpleZookeeperClientMock(), createSimpleKafkaApiUtilsMock())
    }


    def "GetConsumersInfo"() {
        given:
        ConsumerInfo expectedConsumerInfo = createSimpleExpectedConsumerInfo()

        when:
        def consumersInfo = kafkaUtils.getConsumersInfo()

        then:
        consumersInfo[0].topicsInfo[0].logSize == expectedConsumerInfo.topicsInfo[0].logSize
        consumersInfo[0].topicsInfo[0].lag == 5
    }

    def "GetConsumersInfoFromZookeeper"() {
        given:
        ConsumerInfo expectedConsumerInfo = createSimpleExpectedConsumerInfo()

        when:
        def consumersInfo = kafkaUtils.getConsumersInfoFromZookeeper()

        then:
        consumersInfo[0].name == expectedConsumerInfo.name
        consumersInfo[0].topicsInfo[0].offset == expectedConsumerInfo.topicsInfo[0].offset
    }

    def "GetLogSize"() {
        when:
        def logSize = kafkaUtils.getLogSize("test", "topic")

        then:
        logSize == 10
    }

    def "GetLeaderBroker"() {
        when:
        BrokerInfo brokerInfo = kafkaUtils.getLeaderBroker("topic")

        then:
        brokerInfo.id == 1
    }

    def "GetTopicsInfo"() {
        when:
        def topicsInfo = kafkaUtils.getTopicsInfo()

        then:
        topicsInfo[0].name == 'topic'
        topicsInfo[0].leaderId == 1
    }

    def "GetBrokersInfo"() {
        when:
        def brokersInfo = kafkaUtils.getBrokersInfo()

        then:
        brokersInfo[0].host == "host"
        brokersInfo[0].id == 1
        brokersInfo[0].port == 9092
    }

    private ConsumerInfo createSimpleExpectedConsumerInfo() {
        def expectedConsumerInfo = new ConsumerInfo()
        expectedConsumerInfo.name = 'test'
        expectedConsumerInfo.topicsInfo = new ArrayList<>()
        def consumerTopicInfo = new ConsumerTopicInfo()
        consumerTopicInfo.name = 'topic'
        consumerTopicInfo.offset = 5
        consumerTopicInfo.logSize = 10
        expectedConsumerInfo.topicsInfo.add(consumerTopicInfo)
        expectedConsumerInfo
    }

    private MockZookeeperClient createSimpleZookeeperClientMock() {
        return Stub(MockZookeeperClient) {
            getChildren("/consumers") >> ['test']
            getChildren("/consumers/test/offsets") >> ['topic']
            getData("/consumers/test/offsets/topic/0") >> '5'

            getChildren("/brokers/topics") >> ['topic']
            getChildren("/brokers/topics/topic/partitions") >> ['0']
            getJsonData("/brokers/topics/topic/partitions/0/state") >> new JSONObject("{'leader': 1}")

            getChildren("/brokers/ids") >> ['1']
            getJsonData("/brokers/ids/1") >> new JSONObject("{'host': 'host', 'port': 9092}")
        }
    }

    private KafkaApiUtils createSimpleKafkaApiUtilsMock() {
        return Stub(KafkaApiUtils) {
            createSimpleConsumer("host", 9092, _) >> null
            getLastOffset(_, "topic", 0, 1, "test") >> 10
            getOffsetLatestTime() >> 1
        }
    }
}
