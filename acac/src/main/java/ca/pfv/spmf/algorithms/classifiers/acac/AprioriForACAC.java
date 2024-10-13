/* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
* It was obtained from the LAC library under the GNU GPL license, which already contained
* some code from SPMF. Then, it was adapted for SPMF.
* @Copyright original version LAC 2019   @copyright of modifications SPMF 2021
*
* SPMF is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* SPMF is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with SPMF.  If not, see <http://www.gnu.org/licenses/>.
* 
*/
package ca.pfv.spmf.algorithms.classifiers.acac;

import ca.pfv.spmf.algorithms.classifiers.data.Dataset;
import ca.pfv.spmf.algorithms.classifiers.data.Instance;
import ca.pfv.spmf.algorithms.classifiers.data.VirtualDataset;
import ca.pfv.spmf.algorithms.classifiers.general.Item;
import ca.pfv.spmf.algorithms.classifiers.general.Rule;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import org.apache.spark.api.java.function.MapPartitionsFunction;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;


/**
 * Adaptation of the Apriori algorithm for the ACAC algorithm. The Apriori
 * algorithm was presented in:<br/>
 * <br/>
 * 
 * Agrawal, T. Imielinkski, A. Swami. Mining association rules between sets of
 * items in large databases. SIGMOD. 1993. 207-216. This is an adaptation for
 * ACAC algorithm. <br/>
 * <br/>
 * The key differences are:
 * <ul>
 * <li>It searches for class association rules</li>
 * <li>No patterns are generated, but rules are mined directly without an
 * intermediary step for searching for patterns</li>
 * <li>It incorporates the calculation of all-confidence in the mined of class
 * association rules</li>
 * </ul>
 * 
 * @see AlgoACAC
 */
public class AprioriForACAC {
    private JavaRDD<Instance> cachedPartitionRDD;
	private JavaSparkContext sparkContext;
	private final int partition = 6;
	public AprioriForACAC() {

	}

	/**
	 * Extract class association rules from the previously set dataset
	 * 
	 * @param dataset a dataset
	 * @param minSup  the minimum support threshold
	 * @return rules whose support and confidence is greater than a user-specified
	 *         threshold
	 */
	public List<RuleACAC> run(Dataset dataset, double minSup, double minConf, double minAllConf) {
		sparkContext = SparkManager.build();

		// Calculate support relative to the current dataset
		long minSupRelative = (long) Math.ceil(minSup * dataset.getInstances().size());

		// Create a list to store rules
		List<RuleACAC> rules = new ArrayList<RuleACAC>();

		// Find the frequent itemsets of size 1
		List<Item> frequent1 = generateSingletons(dataset, minSupRelative);
//		for(Item item: frequent1){
//			System.out.println(item.toString());
//		}

		// If no frequent item, there are no need to continue searching for larger
		// patterns
		if (frequent1.isEmpty()) {
			return new ArrayList<RuleACAC>();
		}
		
		//Sắp xếp theo thứ tự từ điển
		Collections.sort(frequent1, new Comparator<Item>() {
			public int compare(Item o1, Item o2) {
				return o1.item - o2.item;
			}
		});

		// ====== Recursively try to find larger patterns (having k items) =====
		List<RuleACAC> level = null;
		int k = 2;
		// Generate candidates level by level
		do {
			// if we are going to generate candidates of size 2
			if (k == 2) {
				level = generateAndTestCandidateSize2(dataset, minConf, minAllConf, minSupRelative, rules, frequent1);
			} else {
				// If we are going to generate candidate of a size k > 2
				level = generateAndTestCandidateSizeK(dataset, minConf, minAllConf, minSupRelative, rules, level);
			}
			// Next we will search for candidates of size k+1 if the set of patterns is not
			// empty
			k++;
		} while (!level.isEmpty());

		// Return the rules
//		for(RuleACAC rule: rules){
//			System.out.println(rule);
//		}
//		sparkContext.close();
		return rules;
	}

	/**
	 * Find valid rules of size k
	 * Tạo ra tập luật với kích thước là k bằng những luật từ k - 1
	 * Truyền vào rules từ size 2
	 *
	 * @param dataset        The dataset
	 * @param minConf        The minimum confidence
	 * @param minAllConf     The minimum all-confidence
	 * @param minSupRelative The minimum support (relative value)
	 * @param rules          The set of final rules (will be modified)
	 * @param level         the rules of size k -1
	 * @return The list of rules of size k that are valid (according to ACAC)
	 */
	private List<RuleACAC> generateAndTestCandidateSizeK(Dataset dataset, double minConf, double minAllConf,
														 long minSupRelative, List<RuleACAC> rules, List<RuleACAC> level) {
		// Store the rules of size k-1 in a variable
		List<RuleACAC> previousLevel = level;
//		JavaRDD<RuleACAC> previousLevelRDD = sparkContext.parallelize(previousLevel, partition);
		// Initialize a variable to store rules of size k
		level = new ArrayList<RuleACAC>();
		List<RuleACAC> allRules = new ArrayList<RuleACAC>();


		// For each itemset I1 of size k-1
		for (int i = 0; i < previousLevel.size(); i++) {
			RuleACAC rule1 = previousLevel.get(i);

			// For each itemset I2 of size k-1
			for (int j = i + 1; j < previousLevel.size(); j++) {
				RuleACAC rule2 = previousLevel.get(j);

				// If we cannot combine I1 and I2, then we skip this pair of itemsets
				if (!rule1.isCombinable(rule2))
					continue;

				// Otherwise, we create a new candidate rule by combining itemset1 and itemset2
				RuleACAC newRule = new RuleACAC(rule1); // make a clone
				newRule.add((rule2.get(rule2.size() - 1)));
				newRule.setMaximums(rule1.getSupportRule(), rule2.getSupportRule());

				// If all the subsets of size k-1 of that rule are frequent, we will consider it
				// further
				if (areSubsetsFrequents(newRule, previousLevel)) {
					allRules.add(newRule);
				}

			}
	}

		final Broadcast<Dataset> bcDataset = sparkContext.broadcast(dataset);
		JavaRDD<RuleACAC> rulesRDD = sparkContext.parallelize(allRules, partition);

		JavaRDD<Tuple2<List<RuleACAC>, List<RuleACAC>>>  resultsRDD = evaluateRules(rulesRDD,  bcDataset, minConf, minAllConf, minSupRelative);
		List<Tuple2<List<RuleACAC>, List<RuleACAC>>> collectedResults = resultsRDD.collect();
		for (Tuple2<List<RuleACAC>, List<RuleACAC>> tuple : collectedResults) {
			rules.addAll(tuple._1());
			level.addAll(tuple._2());
		}
		return level;
	}


	/**
	 * Find the rules of size 2
	 * Tìm các rule từ frequent1
	 * Trả về: [6] -> 119 #SUP: 532 #CONF: 0.2407239819004525 #ALLCONF: 1.0
	 * Lấy những rule >= Support relative và >= minAllConfidence
	 * Ví dụ: {Item A = 1, Item B = 1}
	 * @param dataset        The dataset
	 * @param minConf        The minimum confidence
	 * @param minAllConf     The minimum all-confidence
	 * @param minSupRelative The minimum support (relative value)
	 * @param rules          The set of final rules (will be modified)
	 * @param frequent1      The Frequent items.
	 * @return The list of rules of size 2 that are valid (according to ACAC)
	 */
	private List<RuleACAC> generateAndTestCandidateSize2(Dataset dataset, double minConf, double minAllConf,
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

	/**
	 * Generate singletons and its frequency. Only frequent singletons are
	 * considered
	 * Tạo ra các mục đơn lẻ, và tính support
	 * Nếu support của mục đơn lẻ  > minSupRelative thì nhận mục đó
	 * Ví dụ: {Item A = 1}; {Item B = 1}; {Item C = 1}
	 * 
	 * @return the list of frequent items
	 */
	private List<Item> generateSingletons(Dataset dataset, double minSupRelative) {
		// Create a map to count the frequency of each item
		// Key: item Value: Frequency (support)
		Map<Short, Long> mapItemCount = new HashMap<Short, Long>();

		// For each record
		for (Instance instance : dataset.getInstances()) { //Duyệt qua các bản ghi
			Short[] example = instance.getItems();


			// For each attribute that is not the target attribute
			for (int j = 0; j < example.length - 1; j++) { //Duyệt qua các mục trong bản ghi
				short item = example[j];

				// increase the support count
				long count = mapItemCount.getOrDefault(item, 0L);
				mapItemCount.put(item, ++count);
			}
		}

		// Create list of frequent items
		List<Item> frequent1 = new ArrayList<Item>();

		// For each item
		for (Entry<Short, Long> entry : mapItemCount.entrySet()) {
			// If the support is enough
			if (entry.getValue() >= minSupRelative) {
				// Add the item to the list of frequent items
				frequent1.add(new Item(entry.getKey(), entry.getValue()));
			}
		}
		// Return the list of frequent items
		return frequent1;
	}

	/**
	 * Method to check if all the subsets of size k-1 of a candidate are frequent.
	 * That is a requirement of the anti-monotone property of the support
	 * 
	 * @param candidate a candidate rule of size k
	 * @param levelK1   the frequent rules of size k-1
	 * @return true if all the subsets are frequent
	 */
	protected boolean areSubsetsFrequents(Rule candidate, List<RuleACAC> levelK1) {
		// Try removing each item
		for (int positionToRemove = 0; positionToRemove < candidate.getAntecedent().size(); positionToRemove++) {

			boolean found = false;
			// Loop over all rules of size k-1
			for(RuleACAC rule : levelK1) {
				// if the antecedent without the item is the same as the rule of size k-1
				if(sameAs(rule.getAntecedent(), candidate.getAntecedent(),  positionToRemove) == 0) {
					// we found the subset, it is ok
					found = true;
					break;
				}
			}
			// if we did not found the subset then the candidate rule cannot be frequent
			if (!found) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Method to compare two sorted list of integers and see if they are the same,
	 * while ignoring an item from the second list of integer. This methods is used
	 * by some Apriori algorithms.
	 * 
	 * @param itemset1   the first itemset
	 * @param itemsets2  the second itemset
	 * @param posRemoved the position of an item that should be ignored from
	 *                   "itemset2" to perform the comparison.
	 * @return 0 if they are the same, 1 if itemset is larger according to lexical
	 *         order, -1 if smaller.
	 */
	int sameAs(List<Short> itemset1, List<Short> itemsets2, int posRemoved) {
		// a variable to know which item from candidate we are currently searching
		int j = 0;
		// loop on items from "itemset"
		for (int i = 0; i < itemset1.size(); i++) {
			// if it is the item that we should ignore, we skip it
			if (j == posRemoved) {
				j++;
			}
			// if we found the item j, we will search the next one
			if (itemset1.get(i).equals(itemsets2.get(j))) {
				j++;
				// if the current item from i is larger than j,
				// it means that "itemset" is larger according to lexical order
				// so we return 1
			} else if (itemset1.get(i) > itemsets2.get(j)) {
				return 1;
			} else {
				// otherwise "itemset" is smaller so we return -1.
				return -1;
			}
		}
		return 0;
	}

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




}