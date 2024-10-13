package ca.pfv.spmf.algorithms.classifiers.general;

import java.io.Serializable;

/**
 * Class representing an item and its support.
 * Used by some Apriori algorithms like ACN and ACAC
 */
public class Item implements Serializable {
	/** an item */
	public short item;
	/** the item support */
	public long support;
	private static final long serialVersionID = 1L;
	/**
	 * Constructor
	 * @param item the item
	 * @param support its support
	 */
	public Item(short item, long support) {
		this.item = item;
		this.support = support;
	}

	@Override
    public String toString() {
        return "Item{" +
                "item=" + item +
                ", support=" + support +
                '}';
    }
}