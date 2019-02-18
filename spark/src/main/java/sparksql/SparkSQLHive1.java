package sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.Properties;

// 暂时没配置好好。。只能通过spark的thriftserver来访问。。

public class SparkSQLHive1 {
    public static void main(String[] args) throws Exception{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://master:10000");
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select * from test.spark_table1");
        while(rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");

            System.out.println("id : " + id + " , name = " + name + " , age = " + age);
        }

    }
}
