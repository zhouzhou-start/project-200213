package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class Hbase02_DML {
    //TODO 连接集群
    private static Connection connection;
    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","hadoop105,hadoop106,hadoop107");
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //TODO 关闭连接
    public static void close() throws IOException {
        connection.close();
    }
    //TODO 新增和修改数据
    public static void putData(String tableName,String row,String cf,String cn,String value) throws IOException {
        //1.获取DML的Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2.创建Put对象
        Put put = new Put(Bytes.toBytes(row));
        //3.给Put对象添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //4.执行插入数据的操作
        table.put(put);
        //5.释放资源
        table.close();
    }

    //TODO 单条查询数据
    public static void getData(String tableName,String row,String cf,String cn) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2.创建Get对象
        Get get = new Get(Bytes.toBytes(row));
        //指定列族
        get.addFamily(Bytes.toBytes(cf));
        //指定列族:列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //3.查询数据
        Result result = table.get(get);
        //4.解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +"CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    "value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //5.释放资源
        table.close();
    }

//TODO 扫描数据scan
    public static void scanData(String tableName) throws IOException {
        //创建连接对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建scan对象
        Scan scan = new Scan();
        //添加扫描范围
        scan.withStartRow(Bytes.toBytes("1001"));
        scan.withStopRow(Bytes.toBytes("1003"),true);
        //扫描全表
        ResultScanner result = table.getScanner(scan);
        //解析表数据
        Iterator<Result> iterator = result.iterator();
        while(iterator.hasNext()){
            for (Cell cell : iterator.next().rawCells()) {
                System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +"CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //关闭连接
        table.close();
    }

//TODO 删除数据
    public  static void deleteData(String tableName,String row,String cf,String cn) throws IOException {
        //创建链接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建删除对象
        Delete delete = new Delete(Bytes.toBytes(row));
        //指定列族删除数据
        delete.addFamily(Bytes.toBytes(cf));
        //指定列族：列删除数据
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //删除数据
        table.delete(delete);
        //关闭连接
        table.close();
    }

    public static void main(String[] args) throws IOException {
       // putData("student","1002","info1","name","zhouzhou");
        //getData("student","1002","info1","name");
        //scanData("bigdata:student");
        deleteData("bigdata:student","1001","info1","name");
        close();
    }
}
