package com.swh.hbase.table;


import com.swh.hbase.connect.HbaseConnectionUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTableTest {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            connection = HbaseConnectionUtil.getConnection();
            createNamespace(connection, "bigdata-learn");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建命名空间
     *
     * @param connection
     * @param namespace
     * @throws IOException
     */
    private static void createNamespace(Connection connection, String namespace) throws IOException {
        // admin 不是线程安全的
        Admin admin = connection.getAdmin();
        // 命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace)
                .addConfiguration("user", "swh")
                .build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    /**
     * 判断表是否存在
     *
     * @param connection
     * @param namespace
     * @param tableName
     * @return
     * @throws IOException
     */
    private static boolean isExistsTable(Connection connection, String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(namespace, tableName));
        admin.close();
        return b;
    }

    private static void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
                .setMaxVersions(5)
                .build();
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("bigdata-learn", "user"))
                .setColumnFamily(columnFamilyDescriptor)
                .build();
        admin.createTable(tableDescriptor);
        admin.close();
    }

    private static void updateTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableDescriptor userTable = admin.getDescriptor(TableName.valueOf("bigdata-learn", "user"));
        ColumnFamilyDescriptor info = userTable.getColumnFamily(Bytes.toBytes("info"));
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(info)
                .setMaxVersions(4)
                .build();
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(userTable)
                .modifyColumnFamily(columnFamilyDescriptor)
                .build();
        admin.modifyTable(tableDescriptor);
        admin.close();
    }
}
