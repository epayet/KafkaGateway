package com.cdiscount.kafka.zookeeper

import org.apache.zookeeper.ZooKeeper
import spock.lang.Specification

/**
 * Created by emmanuel_payet on 27/11/14.
 */
class ApacheZookeeperClientTest extends Specification {

    def apacheZookeeperClient

    void setup() {
        def zookeeperMock = Stub(ZooKeeper) {
            getChildren("/path", false) >> ["test"]
            getData("/path", false, null) >> "test".getBytes()
            getData("/pathJson", false, null) >> "{'test': 'value'}".getBytes()
        }
        apacheZookeeperClient = new ApacheZookeeperClient(zookeeperMock)
    }

    def "GetChildren"() {
        when:
        def children = apacheZookeeperClient.getChildren("/path")

        then:
        children == ["test"]
    }

    def "GetData"() {
        when:
        def data = apacheZookeeperClient.getData("/path")

        then:
        data == "test"
    }

    def "GetJsonData"() {
        when:
        def data = apacheZookeeperClient.getJsonData("/pathJson")

        then:
        data.get('test') == 'value'
    }
}
