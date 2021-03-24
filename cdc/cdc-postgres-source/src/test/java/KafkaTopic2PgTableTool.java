import com.dtstack.flink.sql.enums.ColumnType;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.RandomStringUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.HostSpec;

import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaTopic2PgTableTool {

    private static String kafkaBootstrapServers = System.getenv("kafkaBootstrapServers");
    private static String[] kafkaTopics = new String[]{};
    private static Set<String> kafkaIgnoreTopics = new HashSet<String>() {{

    }};
    private static String jdbcUrl = System.getenv("pgJdbcUrl");
    private static String database = System.getenv("pgDatabase");
    private static String schema = System.getenv("pgSchema");
    private static String username = System.getenv("pgUsername");
    private static String password = System.getenv("pgPassword");

    private static String sampleApiUrl = System.getenv("sampleApiUrl");

    private static Set<String> jsonCols = new HashSet<>(Arrays.asList("tags", "properties"));

    public static void main(String[] args) throws Exception {
        //获取kafka topic以及表结构信息
        Map<String, Map<String, ColumnType>> topicFieldsMap = fetchKafkaTopicFieldsInfo();

        //生成PgTable
        URI jdbcURI = new URI(jdbcUrl);
        PgConnection pgConnection = new PgConnection(new HostSpec[]{new HostSpec(jdbcURI.getHost(), jdbcURI.getPort())}, username, database, new Properties() {{
            put("password", password);
        }}, "jdbc:" + jdbcUrl);
        for (Map.Entry<String, Map<String, ColumnType>> topicFieldsEntry : topicFieldsMap.entrySet()) {
            createKafkaTopicPgTable(pgConnection, topicFieldsEntry.getKey(), topicFieldsEntry.getValue());
        }

        //生成flinkx同步任务, 一个topic对于一个job
        String writeFlinxJobContentFilePath = "./.yoda/flinkxJobContent";
        topicFieldsMap.entrySet().forEach(entry -> {
            String topicJobStr = buildFlinxJobContent(Collections.singletonMap(entry.getKey(), entry.getValue()));
            System.out.println("write topicJobStr to: " + (writeFlinxJobContentFilePath + "/" + entry.getKey() + ".json"));
            try (FileOutputStream fos = new FileOutputStream(writeFlinxJobContentFilePath + "/" + entry.getKey() + ".json")) {
                fos.write(topicJobStr.getBytes());
                fos.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

    static Map<String, Map<String, ColumnType>> fetchKafkaTopicFieldsInfo() throws Exception {
        Map<String, Map<String, ColumnType>> topicFieldsMap = new HashMap<>();

        String sampleKafkaFieldParamStr = "{\"type\":\"kafka\",\"spec\":{\"ioConfig\":{\"type\":\"kafka\",\"consumerProperties\":{\"bootstrap.servers\":\"%s\"},\"topic\":\"%s\",\"inputFormat\":{\"type\":\"json\",\"keepNullColumns\":true},\"useEarliestOffset\":true},\"dataSchema\":{\"dataSource\":\"sample\",\"timestampSpec\":{\"column\":\"!!!_no_such_column_!!!\",\"missingValue\":\"1970-01-01T00:00:00Z\"},\"dimensionsSpec\":{}},\"type\":\"kafka\",\"tuningConfig\":{\"type\":\"kafka\"}},\"samplerConfig\":{\"numRows\":500,\"timeoutMs\":15000}}";
        for (String topic : kafkaTopics) {
            if (kafkaIgnoreTopics.contains(topic)) {
                continue;
            }

            String sampleResult = KafkaTopicSampleCacheFileUtil.read(topic);
            if (sampleResult == null) {
                sampleResult = doPost(sampleApiUrl, String.format(sampleKafkaFieldParamStr, kafkaBootstrapServers, topic));
                if (sampleResult != null && !sampleResult.isEmpty()) {
                    KafkaTopicSampleCacheFileUtil.write(topic, sampleResult);
                }
            }

            JsonElement sampleResultJson = new JsonParser().parse(sampleResult);
            if (sampleResultJson == null || sampleResultJson.isJsonNull() || sampleResultJson.getAsJsonObject().getAsJsonArray("data").size() == 0) {
                System.out.println("Skip smaple empty data with topic:" + topic);
                continue;
            }

            JsonObject parsedJson = sampleResultJson.getAsJsonObject().getAsJsonArray("data").get(0).getAsJsonObject().getAsJsonObject("parsed");
            for (java.util.Map.Entry<java.lang.String, com.google.gson.JsonElement> entry : parsedJson.entrySet()) {
                if (!topicFieldsMap.containsKey(topic)) {
                    topicFieldsMap.put(topic, new HashMap<>());
                }
                topicFieldsMap.get(topic).put(entry.getKey(), entry.getValue().isJsonPrimitive() ? entry.getValue().getAsJsonPrimitive().isNumber() ? ColumnType.BIGINT : entry.getValue().getAsJsonPrimitive().isBoolean() ? ColumnType.INT : ColumnType.VARCHAR : ColumnType.VARCHAR);
            }

            for (String jsonCol : jsonCols) {
                topicFieldsMap.get(topic).put(jsonCol, ColumnType.VARCHAR);
            }
        }

        return topicFieldsMap;
    }

    static void createKafkaTopicPgTable(PgConnection pgConnection, String tableName, Map<String, ColumnType> tableFieldsMap) throws SQLException {
        StringBuilder createTableSqlSb = new StringBuilder();
        createTableSqlSb.append(String.format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (", schema, tableName.toLowerCase()));

        String createTableFieldSql = tableFieldsMap.entrySet().stream().map(entry -> String.join(" ", getCleanDbColumnName(entry.getKey()), jsonCols.contains(entry.getKey()) ? "JSONB" : entry.getValue().name())).collect(Collectors.joining(","));
        createTableSqlSb.append(createTableFieldSql);

        if (tableFieldsMap.containsKey("id")) {
            createTableSqlSb.append(",primary key (id)");
        }

        createTableSqlSb.append(");");

        pgConnection.execSQLUpdate(createTableSqlSb.toString());
    }

    static String buildFlinxJobContent(Map<String, Map<String, ColumnType>> topicFieldsMap) {
        String flinkxJobContentStr = topicFieldsMap.entrySet().stream().map(topicFieldsEntry -> {
            String jobTopic = topicFieldsEntry.getKey();
            String jobBootstrapServers = kafkaBootstrapServers;
            String jobClientId = RandomStringUtils.random(10, "abcdefg1234567890");
            String jobJdbcUrl = "jdbc:" + jdbcUrl;
            String jobTable = String.format("%s.%s", schema, topicFieldsEntry.getKey().toLowerCase());
            String jobUsername = username;
            String jobPassword = password;
            String jobColumn = topicFieldsEntry.getValue().entrySet().stream().map(topicField -> String.format("{\"name\":\"%s\", \"topic\":\"%s\"}", topicField.getKey(), topicField.getValue().name())).collect(Collectors.joining(","));
            String jobWriteMode = topicFieldsEntry.getValue().containsKey("id") ? "update" : "insert";
            String[] jobArgs = new String[]{jobTopic, jobBootstrapServers, jobClientId, jobJdbcUrl, jobTable, jobUsername, jobPassword, jobColumn, jobWriteMode};

            String topicJobContentStr = "{\n" +
                    "\t\"reader\": {\n" +
                    "\t\t\"parameter\": {\n" +
                    "\t\t\t\"topic\": \"%s\",\n" +
                    "\t\t\t\"groupId\": \"data_pipeline\",\n" +
                    "\t\t\t\"mode\": \"earliest-offset\",\n" +
                    "\t\t\t\"codec\": \"json\",\n" +
                    "\t\t\t\"encoding\": \"UTF-8\",\n" +
                    "\t\t\t\"blankIgnore\": false,\n" +
                    "\t\t\t\"consumerSettings\": {\n" +
                    "\t\t\t\t\"bootstrap.servers\": \"%s\",\n" +
                    "\t\t\t\t\"group.id\": \"data_pipeline\",\n" +
                    "\t\t\t\t\"enable.auto.commit\": \"true\",\n" +
                    "\t\t\t\t\"auto.commit.interval.ms\": \"1000\",\n" +
                    "\t\t\t\t\"client.id\": \"%s\"\n" +
                    "\t\t\t}\n" +
                    "\t\t},\n" +
                    "\t\t\"name\": \"kafkareader\"\n" +
                    "\t},\n" +
                    "\t\"writer\": {\n" +
                    "\t\t\"parameter\": {\n" +
                    "\t\t\t\"connection\": [{\n" +
                    "\t\t\t\t\"jdbcUrl\": \"%s\",\n" +
                    "\t\t\t\t\"table\": [\"%s\"]\n" +
                    "\t\t\t}],\n" +
                    "\t\t\t\"username\": \"%s\",\n" +
                    "\t\t\t\"password\": \"%s\",\n" +
                    "\t\t\t\"column\": [%s],\n" +
                    "\t\t\t\"writeMode\": \"%s\",\n" +
                    "\t\t\t\"batchSize\": 1,\n" +
                    "\t\t\t\"preSql\": [],\n" +
                    "\t\t\t\"postSql\": []\n" +
                    "\t\t},\n" +
                    "\t\t\"name\": \"postgresqlwriter\"\n" +
                    "\t}\n" +
                    "}";
            return String.format(topicJobContentStr, jobArgs);

        }).collect(Collectors.joining(",\n"));

        String flinxJobContentStr = String.format("{\"job\":{\"content\":[%s],\"setting\":{\"restore\":{\"isRestore\":false,\"isStream\":true},\"speed\":{\"readerChannel\":1,\"writerChannel\":1},\"errorLimit\":{\"record\":1,\"percentage\":100.0},\"dirty\":{},\"log\":{}}}}", flinkxJobContentStr);
        return flinxJobContentStr;
    }

    private static String getCleanDbColumnName(String columnName) {
        return columnName.replaceAll("[`:]", "");
    }

    static String doPost(String httpUrl, @Nullable String param) throws Exception {

        StringBuffer result = new StringBuffer();

        //连接

        HttpURLConnection connection = null;

        OutputStream os = null;

        InputStream is = null;

        BufferedReader br = null;

        try {
            // 创建SSLContext
            SSLContext sslContext = SSLContext.getInstance("SSL");
            TrustManager[] tm = {new X509TrustManager() {

                @Override
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                }

                @Override
                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }};
            // 初始化
            sslContext.init(null, tm, new java.security.SecureRandom());
            ;
            // 获取SSLSocketFactory对象
            SSLSocketFactory ssf = sslContext.getSocketFactory();

            //创建连接对象

            URL url = new URL(httpUrl);

            //创建连接

            connection = (HttpURLConnection) url.openConnection();

            if (url.getProtocol().equalsIgnoreCase("https")) {
                ((HttpsURLConnection) connection).setSSLSocketFactory(ssf);
            }

            //设置请求方法

            connection.setRequestMethod("POST");

            //设置连接超时时间

            connection.setConnectTimeout(15000);

            //设置读取超时时间

            connection.setReadTimeout(30000);

            //DoOutput设置是否向httpUrlConnection输出，DoInput设置是否从httpUrlConnection读入，此外发送post请求必须设置这两个

            //设置是否可读取

            connection.setDoOutput(true);

            connection.setDoInput(true);

            //设置通用的请求属性

            connection.setRequestProperty("accept", "*/*");

            connection.setRequestProperty("connection", "Keep-Alive");

            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0;Windows NT 5.1;SV1)");

            connection.setRequestProperty("Content-Type", "application/json;charset=utf-8");

            connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("yoda:Youda".getBytes()));

            //拼装参数

            if (null != param && !param.isEmpty()) {
                //设置参数
                os = connection.getOutputStream();

                //拼装参数
                os.write(param.getBytes("UTF-8"));
                os.flush();
            }

            //设置权限

            //设置请求头等

            //开启连接

            connection.connect();

            //读取响应

            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();

                if (null != is) {

                    br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                    String temp = null;

                    while (null != (temp = br.readLine())) {

                        result.append(temp);

                        result.append("\r\n");

                    }

                }

            }

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            //关闭连接

            if (br != null) {

                try {

                    br.close();

                } catch (IOException e) {

                    e.printStackTrace();

                }

            }

            if (os != null) {

                try {

                    os.close();

                } catch (IOException e) {

                    e.printStackTrace();

                }

            }

            if (is != null) {

                try {

                    is.close();

                } catch (IOException e) {

                    e.printStackTrace();

                }

            }

            //关闭连接

            connection.disconnect();

        }


        return result.toString();

    }

    static class KafkaTopicSampleCacheFileUtil {
        static boolean write(String topic, String sampleData) {
            boolean isSuccess = false;
            try (FileOutputStream fos = new FileOutputStream("./.yoda/kafkaSmapleCache/" + topic)) {
                fos.write(sampleData.getBytes());
                fos.flush();
                isSuccess = true;
            } catch (Exception e) {
            }
            return isSuccess;
        }

        static String read(String topic) {
            String sampleData = null;
            try (FileInputStream fis = new FileInputStream("./.yoda/kafkaSmapleCache/" + topic); BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
                sampleData = br.lines().collect(Collectors.joining("\n"));
            } catch (Exception e) {
            }
            return sampleData;
        }
    }
}
