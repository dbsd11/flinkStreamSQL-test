package com.dtstack.flink.sql.launcher;

import org.apache.calcite.avatica.remote.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class AvaticaTest {

    public static void main(String[] args) throws Exception {
        DriverManager.registerDriver(new Driver());

        String url = "jdbc:avatica:remote:url=https://router.druid.t.gotin.online/druid/v2/sql/avatica/;httpclient_impl=com.gotin.flink.sql.source.avatica.MyAvaticaHttpClient;authentication=BASIC;avatica_user=yoda;avatica_password=Youda";
        Properties connectionProperties = new Properties();
        String query = "-- {\"class\":\"com.gotin.flink.sql.source.avatica.aggregation.GroupConcatJsonAggregator\", \"concatField\":\"pvInfos\", \"jsonKey\":\"key\"} --\n" +
                "select \"`userId`\" as userId, STRING_FORMAT('{\"eventId\":\"%s\",\"page\":\"%s\", \"count\":\"%s\", \"key\":\"%s\"}',\"`eventId`\", \"`jsonObj_t`.p\", count(1), CONCAT(\"`eventId`\",'+',\"`jsonObj_t`.p\")) as pvInfos from \"mid-growingioSdkData\" where __time > CURRENT_DATE and \"`userId`\"='bitdbsd11@163.com' and \"`e`\"='\"pv\"' group by \"`userId`\",\"`eventId`\",\"`jsonObj_t`.p\"";
        try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
            try (
                    final Statement statement = connection.createStatement();
                    final ResultSet resultSet = statement.executeQuery(query)
            ) {
                System.out.println("col1\t\t\tcol2");
                long s = 0;
                while (resultSet.next()) {
                    String col1 = resultSet.getString(1);
                    String col2 = resultSet.getString(2);
                    System.out.println(col1+"\t\t\t"+col2);
                    s++;
                }
                System.out.println("total:"+s);
            }
        }

    }
}
