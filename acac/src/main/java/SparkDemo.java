
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class SparkDemo {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder()
                .appName("demo")
                .config("spark.master", "local")
                .getOrCreate();
        String file_path = "D:\\NCKH\\mushrooms.csv";
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "True")
                .option("inferSchema", "True")
                .load(file_path);


        df.show();
        spark.stop();
    }
}
