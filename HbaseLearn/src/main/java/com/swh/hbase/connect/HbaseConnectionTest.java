package com.swh.hbase.connect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnectionTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","47.109.134.123");
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);
        connection.close();

    }
}
