import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.Properties;

public class SparkMysqlDemo {
    public static void main(String[] args) throws Exception{
        // 1.创建spark streaming环境
        SparkConf conf = new SparkConf().setAppName("SparkKafkaMysqlDroolsDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // 2.配置连接mysql的参数
        String url = "jdbc:mysql://192.168.42.128:3306/SparkKafkaDrools";
        String table = "drools";

        // 3.增加数据库用户名、密码以及驱动
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("driver", "com.mysql.jdbc.Driver");

        // 4.读取表中的数据
        Dataset<Row> df = sqlContext.read().jdbc(url, table, properties).select("drl");

        List<String> listdrl = df.as(Encoders.STRING()).collectAsList();
        System.out.println(listdrl);

    }
}
