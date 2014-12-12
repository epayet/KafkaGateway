package com.cdiscount.kafka.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public interface ZookeeperClient {
    List<String> getChildren(String path) throws ZookeeperException;

    String getData(String path) throws ZookeeperException;

    JSONObject getJsonData(String path) throws ZookeeperException;
}
