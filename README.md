
# SPMF and ACAC

This Project is based on SPMF which was release on 12th June 2024. The SPMF was owned by Philippe Fournier-Viger who was an author of the website which is ***https://www.philippe-fournier-viger.com/spmf*** and the source code is ***[here](https://www.philippe-fournier-viger.com/spmf/index.php?link=download.php)***.

**Note:** This Repository does not own anything and has no affiliation with official SPMF project. We just keep all files that using for ACAC in this repo.

# About

The official SPMF has an ACAC algorithm in Sequential Rule Mining that running in single core. We want to improve performance of this code by using Spark in Parallelism. This is our Goal that we will discuss in this repos.

The ACAC (Associative Classification based on All-confidence) in SPMF which has a bit different concept than an Original[1]. The documents describes how it work is ***[here](https://www.philippe-fournier-viger.com/spmf/ACAC.php)***.

To make it run in Parallelism, we will apply the Spark Library and deploy in single Node.

# Concept run Parallelism

In this section, we will talk about apply parallellism and show the code where we modified the code.

- Create a Spark Context environment to run parallel.
    - We will create new file SparkManager.java following this link acac/src/main/java/ca/pfv/spmf/algorithms/classifiers/acac/SparkManager.java
    ~~~java
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
                        .setMaster("local[1]");

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
    ~~~
    
    The Configuration, **setMaster[]** that we set to Spark Context how many cores that it can use to run.

- we modified the file AprioriForACAC. following the link acac/src/main/java/ca/pfv/spmf/algorithms/classifiers/acac/AprioriForACAC.java

    ~~~java
    private List<RuleACAC> generateAndTestCandidateSize2(JavaSparkContext sparkContext ,Dataset dataset, double minConf, double minAllConf,
                long minSupRelative, List<RuleACAC> rules, List<Item> frequent1) {
            // Create a list to store the rules
            List<RuleACAC> level = new ArrayList<RuleACAC>();

            JavaRDD<Item> createRulesRDD = sparkContext.parallelize(frequent1, partition);
            final Broadcast<Dataset> bcDataset = sparkContext.broadcast(dataset);

            JavaRDD<RuleACAC> createRule = createRulesRDD.mapPartitions(iterator -> {
                List<RuleACAC> localRules = new ArrayList<>();

                while (iterator.hasNext()) {
                    Item item1 = iterator.next();
                    short[] antecedent = new short[]{item1.item};

                    for (int j = 0; j < bcDataset.value().getClassesCount(); j++) {
                        short klass = bcDataset.value().getKlassAt(j);
                        long supportKlass = bcDataset.value().getMapClassToFrequency().getOrDefault(klass, 0L);

                        // Tạo rule mới
                        RuleACAC rule = new RuleACAC(antecedent);
                        rule.setKlass(klass);
                        rule.setMaximums(item1.support, supportKlass); // Dùng item1.support

                        localRules.add(rule);
                    }
                }
                return localRules.iterator();
            });

            List<RuleACAC> allRules = createRule.collect();
            JavaRDD<RuleACAC> rulesRDD = sparkContext.parallelize(allRules, partition);

            JavaRDD<Tuple2<List<RuleACAC>, List<RuleACAC>>>  resultsRDD = evaluateRules(rulesRDD,  bcDataset, minConf, minAllConf, minSupRelative);
            List<Tuple2<List<RuleACAC>, List<RuleACAC>>> collectedResults = resultsRDD.collect();
            for (Tuple2<List<RuleACAC>, List<RuleACAC>> tuple : collectedResults) {
                rules.addAll(tuple._1());
                level.addAll(tuple._2());
            }

            return level;
    }
    ~~~

- **`Input`**:
   - JavaSparkContext sparkContext: Spark object
   - Dataset dataset: dataset
   - double minConf, double minAllConf, long minSupRelative: Parameters to evaluate the formation of rule and level sets
   - List<RuleACAC> rules: Default rule set (From SPMF code)
   - List<Item> frequent1: The resulting items of the generateSingleTon method.


- **`Output`**:
   - Return the level set to proceed in the generateAndTestCandidateSizeK method.
  

- **`Implementation idea`**:

   - Split the results from the generateSingleton method (frequent1) into multiple partitions.
   - Process each partition in parallel using mapPartitions.
   - For each partition, iterate through the values sequentially and create candidate rules from those values.

   
   ~~~ java
  private JavaRDD<Tuple2<List<RuleACAC>, List<RuleACAC>>> evaluateRules (JavaRDD<RuleACAC> rulesRDD,
                                                                         Broadcast<Dataset> bcDataset,
                                                                         double minConf, double minAllConf,
                                                                         long minSupRelative){
      return rulesRDD.mapPartitions(iterator -> {
          List<RuleACAC> localRules = new ArrayList<>();
          List<RuleACAC> localLevel = new ArrayList<>();
          Dataset localDataset = bcDataset.value();

          while (iterator.hasNext()) {
              RuleACAC rule = iterator.next();
              rule.evaluate(localDataset);

              if (rule.getSupportRule() >= minSupRelative && rule.getAllConfidence() >= minAllConf) {
                  if (rule.getConfidence() >= minConf) {
                      localRules.add(rule);
                  } else {
                      localLevel.add(rule);
                  }
              }
          }

      return Collections.singletonList(new Tuple2<>(localRules, localLevel)).iterator();
      });
  }
  ~~~

- **`Input`**: 
  - JavaRDD<RuleACAC> rulesRDD: The candidate rule set created from the generateAndTestCandidateSize2 and generateAndTestCandidateSizeK methods.
  - Broadcast<Dataset> bcDataset: Dataset
  - double minConf, double minAllConf, long minSupRelative: Parameters to evaluate the formation of rule and level sets


- **`Output`**:
  - Rule and level sets


- **`Implementation idea`**:
  - Divide the candidate rule set into multiple partitions. 
  - Perform parallel processing on multiple partitions using mapPartitions.
  - For each partition, iterate through each candidate rule and compute the evaluation.
  - Create conditions to add to the rule set and the level set.

