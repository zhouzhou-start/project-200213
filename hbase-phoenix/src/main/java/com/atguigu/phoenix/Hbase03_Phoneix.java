//package com.atguigu.phoenix;
//
//import org.apache.phoenix.queryserver.client.ThinClientUtil;
//
//import java.sql.*;
//
//public class Hbase03_Phoneix {
//    public static void main(String[] args) throws SQLException {
//
//
//
//        String conn = ThinClientUtil.getConnectionUrl("hadoop105", 8765);
//        Connection connection = DriverManager.getConnection(conn);
//        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
//        ResultSet resultSet = preparedStatement.executeQuery();
//        while(resultSet.next()){
//            System.out.println(resultSet.getString(1)+"\t"+resultSet.getString(2));
//        }
//        resultSet.close();
//        preparedStatement.close();
//        connection.close();
//
//    }
//}
