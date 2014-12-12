package com.cdiscount.kafka.zookeeper;

import org.json.JSONObject;

import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class MockZookeeperClient implements ZookeeperClient {
    private List<String> dataGetChildren;

    @Override
    public List<String> getChildren(String s) throws ZookeeperException{
        return dataGetChildren;
    }

    @Override
    public String getData(String path) throws ZookeeperException {
        return null;
    }

    @Override
    public JSONObject getJsonData(String path) throws ZookeeperException {
        return null;
    }

    public void setDataGetChildren(List<String> dataGetChildren) {
        this.dataGetChildren = dataGetChildren;
    }
}
