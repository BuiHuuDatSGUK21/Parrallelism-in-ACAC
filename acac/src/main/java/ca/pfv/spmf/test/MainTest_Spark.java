package ca.pfv.spmf.test;

import ca.pfv.spmf.algorithms.classifiers.acac.AlgoACAC;
import ca.pfv.spmf.algorithms.classifiers.data.CSVDataset;

import ca.pfv.spmf.algorithms.classifiers.data.Instance;
import ca.pfv.spmf.algorithms.classifiers.data.Attribute;
import ca.pfv.spmf.algorithms.classifiers.data.StringDataset;
import ca.pfv.spmf.algorithms.classifiers.general.ClassificationAlgorithm;
import ca.pfv.spmf.algorithms.classifiers.general.RuleClassifier;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.Entry;

import static ca.pfv.spmf.test.MainTestACAC_batch_holdout.fileToPath;


public class MainTest_Spark {
    public static void main(String[] args) throws Exception {
        String targetClassName = "Class";
        String datasetPath = fileToPath("D:\\acac\\src\\main\\java\\ca\\pfv\\spmf\\test\\Dataset_in_article.txt");
		StringDataset dataset = new StringDataset(datasetPath, targetClassName);
        // Khởi tạo Spark Context
        SparkConf conf = new SparkConf().setAppName("PartitionExample").setMaster("local[1]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);


        // Tạo RDD với 2 partitions
        JavaRDD<Instance> rdd = sparkContext.parallelize(dataset.getInstances(), 2);

        // In ra dữ liệu của từng partition và chỉ số partition
        rdd.mapPartitionsWithIndex((index, partition) -> {
            if (index == 0) {  // Chỉ in ra partition có index là 0
                System.out.println("Partition " + index + " data:");
                partition.forEachRemaining(System.out::println);
            }
            return partition;
        }, true).count();
        sparkContext.stop();
    }
}
