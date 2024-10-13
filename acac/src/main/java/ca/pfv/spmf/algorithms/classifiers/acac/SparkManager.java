package ca.pfv.spmf.algorithms.classifiers.acac;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkManager {
    private static JavaSparkContext sparkContext;

    // Phương thức để xây dựng và cấu hình SparkContext
    public static JavaSparkContext build() {
        if (sparkContext == null) {
            // Tạo SparkConf với các tham số cấu hình
            SparkConf conf = new SparkConf()
                    .setAppName("SparkApp")
                    .setMaster("local[6]");

            sparkContext = new JavaSparkContext(conf);
        }
        return sparkContext;
    }


    public static void close() {
        if (sparkContext != null) {
            sparkContext.close();
            sparkContext = null;
        }
    }
}
