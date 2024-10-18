package ca.pfv.spmf.test;

/* This file is copyright (c) 2021 Philippe Fournier-Viger
* 
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
* 
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* SPMF. If not, see <http://www.gnu.org/licenses/>.
*/

import ca.pfv.spmf.algorithms.classifiers.acac.AlgoACAC;
import ca.pfv.spmf.algorithms.classifiers.acac.SparkManager;
import ca.pfv.spmf.algorithms.classifiers.data.CSVDataset;
import ca.pfv.spmf.algorithms.classifiers.data.Instance;
import ca.pfv.spmf.algorithms.classifiers.data.StringDataset;
import ca.pfv.spmf.algorithms.classifiers.data.VirtualDataset;
import ca.pfv.spmf.algorithms.classifiers.general.ClassificationAlgorithm;
import ca.pfv.spmf.algorithms.classifiers.general.Evaluator;
import ca.pfv.spmf.algorithms.classifiers.general.OverallResults;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Example of how to run the ACAC algorithm 
 *  @author Philippe Fournier-Viger, 2021
 *
 */
public class MainTestACAC_batch_holdout {

	
	public static void main(String[] args) throws Exception {
		/** Khởi động spark để chạy song song */
		JavaSparkContext sc = SparkManager.build();

		/** Tạo một danh sách chứa thời gian từng đợt xử lý */
		List<Integer> times = new ArrayList<>();

		for(int i =1; i <= 5; i++){
			System.out.println("Vòng thứ " + i);
			long startTime = System.currentTimeMillis();
			// We choose "play" as the target attribute that we want to predict using other attributes
//			String targetClassName = "spam";
			String targetClassName = "class";
			//DRK_YN
			// Load the dataset
			String datasetPath = fileToPath("D:\\Github\\Parrallelism-in-ACAC\\acac\\src\\main\\java\\ca\\pfv\\spmf\\test\\mushroomDataSet");
			StringDataset dataset = new StringDataset(datasetPath, targetClassName);
	//
			//  If the dataset is in ARFF format, then use these lines instead:
	//		String datasetPath = fileToPath("weather-train.arff");
	//		ARFFDataset dataset = new ARFFDataset(datasetPath, targetClassName);

			 //If the dataset is in CSV format, then use these lines instead:
			//"D:\sparkDemo\Test_Dataset\smoking_driking_dataset_Ver01.csv"
			//"D:\\sparkDemo\\Test_Dataset\\Spam.csv"
	//		String datasetPath = fileToPath("D:\\sparkDemo\\Test_Dataset\\smoking_driking_dataset_Ver01.csv");
	//		CSVDataset dataset = new CSVDataset(datasetPath, targetClassName);

			// Print stats about the dataset
			dataset.printStats();

			// For debugging (optional)
	//		dataset.printInternalRepresentation();
	//		dataset.printStringRepresentation();
			dataset.PrintKeyAndValue();

			System.out.println("==== Step 2: Training:  Apply the algorithm to build a model (a set of rules) ===");
			// Parameters of the algorithm
			double minSup = 0.1; // = support_threshold / (Số lượng row)
			double minConf = 0.5;
			double minAllConf = 0.5;


			// Create the algorithm
			ClassificationAlgorithm algorithmACAC = new AlgoACAC(sc, minSup, minConf, minAllConf);

			// We create an object Evaluator to run the experiment using k-fold cross validation
			Evaluator experiment1 = new Evaluator();

			// The following line indicates that 50% of the dataset will be used for training.
			// The rest of the dataset will be used for testing.
			double percentage = 0.5;


			// We run the experiment
			ClassificationAlgorithm[] algorithms = new ClassificationAlgorithm[] {algorithmACAC};
			OverallResults allResults = experiment1.trainAndRunClassifiersHoldout(algorithms, dataset, percentage);

			// Save statistics about the execution to files (optional)
			String forTrainingPath = "acac\\outputReportForTraining.txt";
			String onTrainingPath = "acac\\outputReportOnTraining.txt";
			String onTrestingPath = "acac\\outputReportOnTesting.txt";
			allResults.saveMetricsResultsToFile(forTrainingPath, onTrainingPath, onTrestingPath);

			// Print statistics to the console (optional)
			allResults.printStats();
			long endtime = System.currentTimeMillis();
			System.out.println("Thời gian chạy: " + (endtime-startTime));
			times.add((int) (endtime - startTime));
		}

		int sum=0;
		for(Integer t : times){
			System.out.println(t);
			sum += t;
		}
		System.out.println("Thời trung bình tất cả lần chạy: " + (sum / times.size()));
	}


	public static String fileToPath(String filename) throws UnsupportedEncodingException{
//		URL url = MainTestACAC_batch_holdout.class.getResource(filename);
//		 return java.net.URLDecoder.decode(url.getPath(),"UTF-8");
		return filename;
	}
}
