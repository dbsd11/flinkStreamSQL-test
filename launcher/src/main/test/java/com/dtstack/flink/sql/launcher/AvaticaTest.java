package com.dtstack.flink.sql.launcher;

import org.apache.calcite.avatica.remote.Driver;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class AvaticaTest {

    public static void main(String[] args) throws Exception {
        DriverManager.registerDriver(new Driver());

        String url = "jdbc:avatica:remote:url=https://router.druid.t.gotin.online/druid/v2/sql/avatica/;httpclient_impl=com.gotin.flink.sql.source.avatica.MyAvaticaHttpClient;authentication=BASIC;avatica_user=yoda;avatica_password=Youda";
        Properties connectionProperties = new Properties();
        String query = "-- {\"class\":\"com.gotin.flink.sql.source.avatica.aggregation.GroupConcatJsonAggregator\", \"concatField\":\"loginInfos\", \"jsonKey\":\"loginTime\"} --\n" +
                "select \"`userId`\" as userId,\"`ext`\" as loginInfos from \"mid-eventUserAction\" where __time > CURRENT_DATE and \"`action`\"='login' and \"`userId`\"=?";
        try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            for (int i = 0; i < 10; i++) {
//                try (
//                        final Statement statement = connection.createStatement();
//                        final ResultSet resultSet = preparedStatement.executeQuery()
//                )
                try {
                    preparedStatement.clearParameters();
                    JdbcUtils.setField(preparedStatement, JdbcTypeUtil.typeInformationToSqlType(TypeInformation.of(String.class)), "15726681696", 0);
                    final ResultSet resultSet = preparedStatement.executeQuery();

                    System.out.println("col1\t\t\tcol2");
                    long s = 0;
                    while (resultSet.next()) {
                        String col1 = resultSet.getString(1);
                        String col2 = resultSet.getString(2);
                        System.out.println(col1 + "\t\t\t" + col2);
                        s++;
                    }
                    System.out.println("total:" + s);
                } finally {
                }
            }
        }

    }
}
