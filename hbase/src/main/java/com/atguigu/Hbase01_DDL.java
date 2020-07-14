package com.atguigu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public class Hbase01_DDL {
    //获取连接以及DDL对象
    private static Connection connection;
    private static Admin admin;
    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","hadoop105,hadoop106,hadoop107");
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//关闭连接资源
    public  static void close() throws IOException {
        admin.close();
        connection.close();
    }
//创建命名空间
    public static void createNS(String sn) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(sn).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (Exception e) {
            System.out.println("命名空间已存在");;
        }
        admin.close();
        connection.close();
    }
//判断表是否存在
    public static Boolean isExistTable(String nameTable) throws IOException {
        return admin.tableExists(TableName.valueOf(nameTable));
    }

    //创建表
    public static void createTable(String tableName,String... cfs) throws IOException {
       if(cfs.length<=0){
           System.out.println("请输入列族信息！！！");
           return;
       }else if(isExistTable(tableName)){
           System.out.println(tableName + "该表已存在！");
           return;
       }else {
           //3.创建表描述器Builder对象
           TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
           //循环放入列族信息
           for (String cf : cfs) {
               //创建列族描述器
               ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
               tableDescriptorBuilder.setColumnFamily(build);
           }
           //创建表描述器
           tableDescriptorBuilder.setCoprocessor("com.atguigu.Hbase03_ConProcessor");
           TableDescriptor build = tableDescriptorBuilder.build();
           //创建表
           admin.createTable(build);
       }
    }

    //删除表
    public static void deleteTable(String tableName) throws IOException {
        //判断表是否存在
        if(!isExistTable(tableName)){
            System.out.println("该表不存在");
            return;
        }
        TableName name = TableName.valueOf(tableName);
        //使表不可用
        admin.disableTable(name);
        //删除表操作
        admin.deleteTable(name);
    }


    public static void main(String[] args) throws IOException {
      //createNS("bigtable");
       // System.out.println(isExistTable("stu"));
       createTable("student1","info1");
        //deleteTable("student1");
        close();
    }
}
