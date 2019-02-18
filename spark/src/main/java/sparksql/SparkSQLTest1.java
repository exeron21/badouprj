package sparksql;/*package sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SparkSQLTest1 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("sparkSqlJava")
                .getOrCreate();

        Dataset df = ss.read().json("E:\\spark_test_file\\json1.txt");
        // 创建临时视图
        df.createOrReplaceTempView("df");

        ss.sql("select * from df where age >13").show();
        df.select("id", "age").show();
        df.where("age>15").show();

        // 聚合查询
        ss.sql("select count(1) from df where age>14").show();

        JavaRDD<Row> rdd1 = df.toJavaRDD();
        rdd1.collect().forEach(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                long age = row.getLong(0);
                long id = row.getLong(1);
                String name = row.getString(2);
                System.out.println(id + ":" + age + ":" + name);
            }
        });

        *//**
        JavaRDD<String> rdd2 = ss.sparkContext().textFile("E:\\spark_test_file\\json1.txt",1)
                .toJavaRDD();

        String schema = "id,name,age";
        List<StructField> fields = new ArrayList<>();
        for (String field : schema.split(",")) {
            StructField f = DataTypes.createStructField(field, DataTypes.StringType, true);
            fields.add(f);
        }
        StructType schema2 = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = rdd2.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attrs = record.split(",");
                return RowFactory.create(attrs[0], attrs[1].trim(),attrs[2]);
            }
        });

        Dataset<Row> peopleDf = ss.createDataFrame(rowRDD,schema2);
        peopleDf.createOrReplaceTempView("people");
        Dataset<Row> result = ss.sql("select * from people");
        result.show();
         *//*

        df.write().mode(SaveMode.Append).json("E:\\spark_test_file\\df.json");
    }
}*/
