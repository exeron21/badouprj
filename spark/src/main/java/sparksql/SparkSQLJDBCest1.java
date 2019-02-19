package sparksql;

import org.apache.spark.sql.*;

import java.util.Properties;
import java.util.function.Consumer;

// 这是spark通用jdbc来访问
public class SparkSQLJDBCest1 {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("sparkSqlJava")
                .getOrCreate();

        // 基本的sparksql-jdbc查询
        String url = "jdbc:mysql://localhost:3306/test";
        String driver = "com.mysql.jdbc.Driver";
        String user = "root";
        String password = "root";
        String tname = "item";
        Dataset<Row> df = session.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable",tname)
                .option("user", user)
                .option("password", password)
                .option("driver", driver)
                .load();

        df.show();

        // 将生成的dataset重新写入jdbc
        Dataset<Row> df2 = df.select("id", "itemname").where("id>4");
        df2 = df2.distinct();
        df2.show();
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password","root");
        properties.put("driver","com.mysql.jdbc.Driver");

        // 表若不存在时会自动创建
        df2.write().jdbc(url,"item_2",properties);
    }
}
