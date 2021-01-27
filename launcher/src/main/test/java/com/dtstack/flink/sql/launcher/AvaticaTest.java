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
        String query = "-- {\"class\":\"com.gotin.flink.sql.source.avatica.aggregation.GroupConcatAggregator\", \"concatField\":\"info\"} --\n select * from (\n" +
                "  select 'bitdbsd11@163.com' as userId, CAST(STRING_FORMAT('{\"eventId\":\"%s\",\"action\":\"%s\", \"count\":\"%s\"}',\"`eventId`\", \"`action`\", cnt) as VARCHAR) as info from (select \"`eventId`\",\"`action`\",count(1) as cnt from \"mid-eventUserAction\" where __time > CURRENT_DATE group by \"`eventId`\",\"`action`\") group by \"`eventId`\", \"`action`\", cnt\n" +
                ")";
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
