package com.persist.util.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;

/**
 * Created by taozhiheng on 16-7-12.
 *
 * helper to handle hadoop hbase
 *
 */
public class HBaseHelper {


    private Connection mConnection;
    private final static String QUORUM = "hbase.zookeeper.quorum";
    private final static String PORT = "hbase.zookeeper.property.clientPort";
    private final static String MASTER = "hbase.master";
    private final static String AUTHENTICATION = "hbase.security.authentication";

    public HBaseHelper(String quorum, int port)
    {
        this(quorum, port, null, null);
    }

    public HBaseHelper(String quorum, int port, String master, String auth)
    {
        Configuration config = HBaseConfiguration.create();
        config.set(QUORUM, quorum);
        config.set(PORT, Integer.toString(port));
//        config.set(MASTER, "master:60000");
//        config.set(AUTHENTICATION, auth);
        try {
            mConnection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 创建数据库表
    public void createTable(String tableName, String[] columnFamilies)
            throws Exception {
        // 新建一个数据库管理员
        Admin hAdmin = mConnection.getAdmin();
        TableName t = TableName.valueOf(tableName);
        if (hAdmin.tableExists(t)) {
//            System.out.println("表已经存在");
//            System.exit(0);
            return;
        } else
        {
            // 新建一个 scores 表的描述
            HTableDescriptor tableDesc = new HTableDescriptor(t);
            // 在描述里添加列族
            for (String columnFamily : columnFamilies) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            // 根据配置好的描述建表
            hAdmin.createTable(tableDesc);
//            System.out.println("创建表成功");
        }
    }

    // 删除数据库表
    public void deleteTable(String tableName) throws Exception {
        // 新建一个数据库管理员
        System.out.println("delete table:"+tableName);
        Admin hAdmin = mConnection.getAdmin();
        TableName t = TableName.valueOf(tableName);
        if (hAdmin.tableExists(t)) {
            // 关闭一个表
            hAdmin.disableTable(t);
            // 删除一个表
            hAdmin.deleteTable(t);
//            System.out.println("删除表成功");

        } else {
//            System.out.println("删除的表不存在");
//            System.exit(0);
        }
    }

    // 添加一条数据
    public void addRow(String tableName, String row,
                              String columnFamily, String[] columns, String[] values) throws Exception {
        Table table = mConnection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        // 参数出分别：列族、列、值
        for(int i = 0; i < columns.length; i++)
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        table.put(put);
    }

    // 删除一条数据
    public void delRow(String tableName, String row) throws Exception {
        Table table = mConnection.getTable(TableName.valueOf(tableName));
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    // 删除多条数据
    public void delMultiRows(String tableName, String[] rows)
            throws Exception {
        Table table = mConnection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<Delete>();

        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            list.add(del);
        }

        table.delete(list);
    }

    // get row
    public Result getRow(String tableName, String row) throws Exception {
        Table table = mConnection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(row));
        return table.get(get);
//        Result result = table.get(get);
//        System.out.println(Bytes.toString(result.getRow()));
    }

    // get all records
    public ResultScanner getAllRows(String tableName) throws Exception {
        Table table = mConnection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        return table.getScanner(scan);
//        ResultScanner results = table.getScanner(scan);
        // 输出结果
//        for (Result result : results) {
//            System.out.println(Bytes.toString(result.getRow()));

//            for (KeyValue rowKV : result.raw()) {
//                System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
//                System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
//                System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
//                System.out
//                        .print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
//                System.out.println("Value: " + new String(rowKV.getValue()) + " ");
//            }
//        }
    }

    public List getAllTables() {
        List<String> tables = null;
            try
            {
                Admin admin = mConnection.getAdmin();

                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
//                    System.out.println(hTableDescriptor.getNameAsString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        return tables;
    }

    public void close()
    {
        try {
            if(mConnection != null)
                mConnection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
