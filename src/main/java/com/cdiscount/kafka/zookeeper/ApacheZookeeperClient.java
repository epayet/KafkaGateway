package com.cdiscount.kafka.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class ApacheZookeeperClient implements ZookeeperClient {
    private final ZooKeeper zooKeeper;

    public ApacheZookeeperClient(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public ApacheZookeeperClient(String connectionString) throws IOException {
        zooKeeper = new ZooKeeper(connectionString, 10000, null);
    }

    public List<String> getChildren(String path) throws ZookeeperException{
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException e) {
            throw new ZookeeperException(e);
        } catch (InterruptedException e) {
            throw new ZookeeperException(e);
        }
    }

    public String getData(String path) throws ZookeeperException {
        try {
            return new String(zooKeeper.getData(path, false, null));
        } catch (KeeperException e) {
            throw new ZookeeperException(e);
        } catch (InterruptedException e) {
            throw new ZookeeperException(e);
        }
    }

    public JSONObject getJsonData(String path) throws ZookeeperException {
        return new JSONObject(getData(path));
    }
}
