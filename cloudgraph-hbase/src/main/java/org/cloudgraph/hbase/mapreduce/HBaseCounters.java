package org.cloudgraph.hbase.mapreduce;

public interface HBaseCounters {
	/** HBase Counter Group */
	public static final String HBASE_COUNTER_GROUP_NAME = "HBase Counters";

	/** Scan restarts counter for HBase */
	public static final String HBASE_COUNTER_NAME_NUM_SCANNER_RESTARTS = "NUM_SCANNER_RESTARTS";

}
