package org.cloudgraph.hbase.util;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;

/**
 * HBase filter debugging utilities
 * @author Scott Cinnamond
 * @since 0.5
 */
public class FilterUtil {
	
	public static String printFilterTree(Filter filter) throws IOException {
    	StringBuilder buf = new StringBuilder();
    	printFilterTree(filter, buf, 0);
        return buf.toString();
    }
    
	public static void printFilterTree(Filter filter, StringBuilder buf, int level) throws IOException {
        if (filter instanceof FilterList) {
        	buf.append("\n");
        	for (int i = 0; i < level; i++)
        		buf.append("\t");
        	FilterList list = (FilterList)filter;
        	buf.append("[LIST:");
        	buf.append(list.getOperator().name());
        	for (Filter child : list.getFilters()) {
        		printFilterTree(child, buf, level + 1);
        	}
        	buf.append("\n");
        	for (int i = 0; i < level; i++)
        		buf.append("\t");
        	buf.append("]");
        }
        else {
        	buf.append("\n");
        	for (int i = 0; i < level; i++)
        		buf.append("\t");
        	if (filter instanceof CompareFilter) {
        		CompareFilter compFilter = (CompareFilter)filter;
        	    buf.append(compFilter.getClass().getSimpleName() + ": ");
        	    buf.append(compFilter.getOperator().name());
        	    buf.append(" ");
        	    buf.append(new String(compFilter.getComparator().getValue()));
        	}
        	else if (filter instanceof MultipleColumnPrefixFilter) {
        		MultipleColumnPrefixFilter prefixFilter = (MultipleColumnPrefixFilter)filter;
        	    buf.append(filter.getClass().getSimpleName()+": ");
        		byte[][] prefixes = prefixFilter.getPrefix();
            	for (int i = 0; i<prefixes.length; i++) {
        		    String prefix = new String(prefixes[i]);
                	buf.append("\n");
                	for (int j = 0; j < level+1; j++)
                		buf.append("\t");
                	buf.append(prefix);
            	}
        	}
        	else if (filter instanceof RandomRowFilter) {
        		RandomRowFilter rowFilter = (RandomRowFilter)filter;
        	    buf.append(filter.getClass().getSimpleName()+": ");
        	    buf.append(rowFilter.getChance());
        	}
        	else {
        	    buf.append(filter.toString());  
        	}
        }
    }

}
