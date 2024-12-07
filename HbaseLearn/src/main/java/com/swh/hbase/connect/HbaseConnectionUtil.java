package com.swh.hbase.connect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public final class HbaseConnectionUtil {
    private static Connection INSTANCE;

    public static synchronized Connection getConnection() {
        if (INSTANCE != null) {
            return INSTANCE;
        } else {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "47.109.134.123");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.master", "47.109.134.123:16010");
            // 设置zookeeper的连接超时时间，单位是毫秒
            conf.set("zookeeper.session.timeout", "10000");
            // 设置rpc的超时时间，单位是毫秒
            conf.set("hbase.rpc.timeout", "10000");
            // 设置hbase客户端操作的全局超时时间，单位是毫秒
            conf.set("hbase.client.operation.timeout", "10000");
            try {
                return ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
