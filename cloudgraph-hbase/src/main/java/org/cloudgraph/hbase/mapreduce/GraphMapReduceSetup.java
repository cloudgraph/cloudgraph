/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.mapreduce.JarFinder;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.hadoop.hbase.mapreduce.hadoopbackport.JarFinder;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.Config;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.filter.GraphFetchColumnFilterAssembler;
import org.cloudgraph.hbase.filter.HBaseFilterAssembler;
import org.cloudgraph.hbase.io.DistributedGraphReader;
import org.cloudgraph.hbase.scan.CompleteRowKey;
import org.cloudgraph.hbase.scan.FuzzyRowKey;
import org.cloudgraph.hbase.scan.PartialRowKey;
import org.cloudgraph.hbase.scan.PartialRowKeyScanAssembler;
import org.cloudgraph.hbase.scan.ScanCollector;
import org.cloudgraph.hbase.scan.ScanRecognizerSyntaxTreeAssembler;
import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.job.JobSetup;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.state.SimpleStateMarshallingContext;
import org.cloudgraph.state.StateMarshalingContext;
import org.cloudgraph.state.StateNonValidatingDataBinding;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.From;
import org.plasma.query.Query;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

import com.google.protobuf.InvalidProtocolBufferException;
import commonj.sdo.Type;

/**
 * Mapreduce Job setup utility for {@link GraphMapper} and {@link GraphReducer} and 
 * extensions.
 */
@SuppressWarnings("unchecked")
public class GraphMapReduceSetup extends JobSetup {
	static Log LOG = LogFactory.getLog(GraphMapReduceSetup.class);

	/**
	 * Use this before submitting a graph map job. It will appropriately set up
	 * the job.
	 * 
	 * @param query The query defining the {@link GraphMapper} input graphs. 
	 * @param mapper
	 *            The mapper class to use.
	 * @param outputKeyClass
	 *            The class of the output key.
	 * @param outputValueClass
	 *            The class of the output value.
	 * @param job
	 *            The current job to adjust. Make sure the passed job is
	 *            carrying all necessary HBase configuration.
	 * @throws IOException
	 *             When setting up the details fails.
	 */
	public static void setupGraphMapperJob(Query query,
			Class<? extends GraphMapper> mapper,
			Class<? extends WritableComparable> outputKeyClass,
			Class<? extends Writable> outputValueClass, Job job)
			throws IOException {
		setupGraphMapperJob(query, mapper, outputKeyClass, outputValueClass,
				job, true);
	}

	/**
	 * Use this before submitting a graph map job. It will appropriately set up
	 * the job.
	 * 
	 * @param query The query defining the {@link GraphMapper} input graphs. 
	 * @param mapper
	 *            The mapper class to use.
	 * @param outputKeyClass
	 *            The class of the output key.
	 * @param outputValueClass
	 *            The class of the output value.
	 * @param job
	 *            The current job to adjust. Make sure the passed job is
	 *            carrying all necessary HBase configuration.
	 * @param addDependencyJars
	 *            upload HBase jars and jars for any of the configured job
	 *            classes via the distributed cache (tmpjars).
	 * @throws IOException
	 *             When setting up the details fails.
	 */
	public static void setupGraphMapperJob(Query query,
			Class<? extends GraphMapper> mapper,
			Class<? extends WritableComparable> outputKeyClass,
			Class<? extends Writable> outputValueClass, Job job,
			boolean addDependencyJars,
			Class<? extends InputFormat> inputFormatClass) throws IOException {
		job.setInputFormatClass(inputFormatClass);
		if (outputValueClass != null)
			job.setMapOutputValueClass(outputValueClass);
		if (outputKeyClass != null)
			job.setMapOutputKeyClass(outputKeyClass);
		job.setMapperClass(mapper);
		Configuration conf = job.getConfiguration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

		PlasmaType type = getRootType(query);

		Where where = query.getModel().findWhereClause();
		SelectionCollector selectionCollector = null;
		if (where != null)
			selectionCollector = new SelectionCollector(query.getModel()
					.getSelectClause(), where, type);
		else
			selectionCollector = new SelectionCollector(query.getModel()
					.getSelectClause(), type);
		selectionCollector.setOnlyDeclaredProperties(false);
		// FIXME: generalize collectRowKeyProperties
		for (Type t : selectionCollector.getTypes())
			collectRowKeyProperties(selectionCollector, (PlasmaType) t);

		StateMarshalingContext marshallingContext = null;
		try {
			marshallingContext = new SimpleStateMarshallingContext(
					new StateNonValidatingDataBinding());
		} catch (JAXBException e) {
			throw new GraphServiceException(e);
		} catch (SAXException e) {
			throw new GraphServiceException(e);
		}

		DistributedGraphReader graphReader = new DistributedGraphReader(type,
				selectionCollector.getTypes(), marshallingContext);

		HBaseFilterAssembler columnFilterAssembler = new GraphFetchColumnFilterAssembler(
				selectionCollector, type);
		Filter columnFilter = columnFilterAssembler.getFilter();

		From from = query.getModel().getFromClause();
		List<Scan> scans = createScans(from, where, type, columnFilter, conf);

		conf.set(GraphInputFormat.QUERY, marshal(query));
		conf.set(GraphInputFormat.ROOT_TABLE, graphReader.getRootTableReader()
				.getTableName());
		
	    List<String> scanStrings = new ArrayList<String>();

	    for (Scan scan : scans) {
	    	scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, 
	    		Bytes.toBytes(graphReader.getRootTableReader().getTableName()));
	        scanStrings.add(convertScanToString(scan));
	    }
	    conf.setStrings(GraphInputFormat.SCANS,
	          scanStrings.toArray(new String[scanStrings.size()]));
		

		if (addDependencyJars) {
			addDependencyJars(job);
		}
		initCredentials(job);
	}
	
	private static List<Scan> createScans(From from, Where where, PlasmaType type,
			Filter columnFilter, Configuration conf) throws IOException {

		List<Scan> result = new ArrayList<Scan>();
		if (where == null) {
			conf.setBoolean(GraphInputFormat.RECOGNIZER, false);
			Scan scan = createDefaultScan(from, type, columnFilter);
			result.add(scan);
			return result;
		}
		
		ScanRecognizerSyntaxTreeAssembler recognizerAssembler = 
			new ScanRecognizerSyntaxTreeAssembler(where, type);
		Expr scanRecognizerRootExpr = recognizerAssembler.getResult();
		if (LOG.isDebugEnabled()) {
			ExprPrinter printer = new ExprPrinter();
			scanRecognizerRootExpr.accept(printer);
			LOG.debug("Scan Recognizer: " + printer.toString());
		}
		ScanCollector scanCollector = new ScanCollector(type);
		scanRecognizerRootExpr.accept(scanCollector);
		List<PartialRowKey> partialScans = scanCollector.getPartialRowKeyScans();
		List<FuzzyRowKey> fuzzyScans = scanCollector.getFuzzyRowKeyScans();
		List<CompleteRowKey> completeKeys = scanCollector.getCompleteRowKeys();
		if (!scanCollector.isQueryRequiresGraphRecognizer()) {
			conf.setBoolean(GraphInputFormat.RECOGNIZER, false);
			scanRecognizerRootExpr = null;
		}
		else {
			conf.setBoolean(GraphInputFormat.RECOGNIZER, true);
		}

		if (completeKeys.size() > 0)
			throw new GraphServiceException(
					"expected no complete key values");
		//FIXME: table split by region entirely based on partial key start stop bytes
		for (FuzzyRowKey fuzzyKey : fuzzyScans) {
			Scan scan = createScan(fuzzyKey, columnFilter);
			result.add(scan);
		}
		for (PartialRowKey partialKey : partialScans) {
			Scan scan = createScan(partialKey, columnFilter);
			result.add(scan);
		}
		
		if (result.size() == 0)
		{
			Scan scan = createDefaultScan(from, type, columnFilter);
			result.add(scan);
		}

		return result;
	}
	
	private static Scan createDefaultScan(From from, PlasmaType type, Filter columnFilter) {
		Scan scan;
		
		PartialRowKeyScanAssembler scanAssembler = new PartialRowKeyScanAssembler(type);
		scanAssembler.assemble();
		byte[] startKey = scanAssembler.getStartKey();
		if (startKey != null && startKey.length > 0) {
            LOG.warn("using default graph partial "
            		+ "key scan - could result in very large results set");
            scan = createScan(scanAssembler, columnFilter);
		}
		else {
			Float sample = from.getRandomSample();
			if (sample == null) {
	            FilterList rootFilter = new FilterList(
	        			FilterList.Operator.MUST_PASS_ALL);
	            scan = new Scan();
	            rootFilter.addFilter(columnFilter);
	            scan.setFilter(rootFilter);        
			    LOG.warn("query resulted in no filters or scans - using full table scan - " 
        	            + "could result in very large results set");
			}
    		else {
	            FilterList rootFilter = new FilterList(
	        			FilterList.Operator.MUST_PASS_ALL);
	   			RandomRowFilter rowFilter = new RandomRowFilter(sample);
	   			rootFilter.addFilter(rowFilter);
	            rootFilter.addFilter(columnFilter);
  			    scan = new Scan();
    			scan.setFilter(rootFilter);     
     			LOG.warn("using random-sample scan ("+sample+") - " 
        	            + "could result in very large results set");
    		}
		}
		return scan;		
	}

	/**
	 * Use this before submitting a graph map job. It will appropriately set up
	 * the job.
	 * 
	 * @param query The query defining the {@link GraphMapper} input graphs. 
	 * @param mapper
	 *            The mapper class to use.
	 * @param outputKeyClass
	 *            The class of the output key.
	 * @param outputValueClass
	 *            The class of the output value.
	 * @param job
	 *            The current job to adjust. Make sure the passed job is
	 *            carrying all necessary HBase configuration.
	 * @param addDependencyJars
	 *            upload HBase jars and jars for any of the configured job
	 *            classes via the distributed cache (tmpjars).
	 * @throws IOException
	 *             When setting up the details fails.
	 */
	public static void setupGraphMapperJob(Query query,
			Class<? extends GraphMapper> mapper,
			Class<? extends WritableComparable> outputKeyClass,
			Class<? extends Writable> outputValueClass, Job job,
			boolean addDependencyJars) throws IOException {
		setupGraphMapperJob(query, mapper, outputKeyClass, outputValueClass,
				job, addDependencyJars, GraphInputFormat.class);
	}

	  public static void initCredentials(Job job) throws IOException {
		    UserProvider userProvider = UserProvider.instantiate(job.getConfiguration());
		    if (userProvider.isHadoopSecurityEnabled()) {
		      // propagate delegation related props from launcher job to MR job
		      if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
		        job.getConfiguration().set("mapreduce.job.credentials.binary",
		                                   System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		      }
		    }

		    if (userProvider.isHBaseSecurityEnabled()) {
		      try {
		        // init credentials for remote cluster
		        String quorumAddress = job.getConfiguration().get(TableOutputFormat.QUORUM_ADDRESS);
		        User user = userProvider.getCurrent();
		        if (quorumAddress != null) {
		          Configuration peerConf = HBaseConfiguration.createClusterConf(job.getConfiguration(),
		              quorumAddress, TableOutputFormat.OUTPUT_CONF_PREFIX);
		          Connection peerConn = ConnectionFactory.createConnection(peerConf);
		          try {
		            TokenUtil.addTokenForJob(peerConn, user, job);
		          } finally {
		            peerConn.close();
		          }
		        }

		        Connection conn = ConnectionFactory.createConnection(job.getConfiguration());
		        try {
		          TokenUtil.addTokenForJob(conn, user, job);
		        } finally {
		          conn.close();
		        }
		      } catch (InterruptedException ie) {
		        LOG.info("Interrupted obtaining user authentication token");
		        Thread.currentThread().interrupt();
		      }
		    }
		  }

	  /**
	   * Writes the given scan into a Base64 encoded string.
	   *
	   * @param scan  The scan to write out.
	   * @return The scan saved in a Base64 encoded string.
	   * @throws IOException When writing the scan fails.
	   */
	  static String convertScanToString(Scan scan) throws IOException {
	    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
	    return Base64.encodeBytes(proto.toByteArray());
	  }

	  /**
	   * Converts the given Base64 string back into a Scan instance.
	   *
	   * @param base64  The scan details.
	   * @return The newly created Scan instance.
	   * @throws IOException When reading the scan instance fails.
	   */
	  static Scan convertStringToScan(String base64) throws IOException {
	    byte [] decoded = Base64.decode(base64);
	    ClientProtos.Scan scan;
	    try {
	      scan = ClientProtos.Scan.parseFrom(decoded);
	    } catch (InvalidProtocolBufferException ipbe) {
	      throw new IOException(ipbe);
	    }

	    return ProtobufUtil.toScan(scan);
	  }
	

	/**
	 * Use this before submitting a graph reduce job. It will appropriately set
	 * up the JobConf.
	 * 
	 * @param tableConfig
	 *            The output table.
	 * @param reducer
	 *            The reducer class to use.
	 * @param job
	 *            The current job to adjust.
	 * @throws IOException
	 *             When determining the region count fails.
	 */
	public static void setupGraphReducerJob(Query query,
			Class<? extends GraphReducer> reducer, Job job) throws IOException {
		setupGraphReducerJob(query, reducer, job, null);
	}

	/**
	 * Use this before submitting a graph reduce job. It will appropriately set
	 * up the JobConf.
	 * 
	 * @param tableConfig
	 *            The output table.
	 * @param reducer
	 *            The reducer class to use.
	 * @param job
	 *            The current job to adjust.
	 * @param partitioner
	 *            Partitioner to use. Pass <code>null</code> to use default
	 *            partitioner.
	 * @throws IOException
	 *             When determining the region count fails.
	 */
	public static void setupGraphReducerJob(Query query,
			Class<? extends GraphReducer> reducer, Job job, Class partitioner)
			throws IOException {
		setupGraphReducerJob(query, reducer, job, partitioner, null, null, null);
	}

	/**
	 * Use this before submitting a graph reduce job. It will appropriately set
	 * up the JobConf.
	 * 
	 * @param query
	 *            The output query.
	 * @param reducer
	 *            The reducer class to use.
	 * @param job
	 *            The current job to adjust. Make sure the passed job is
	 *            carrying all necessary HBase configuration.
	 * @param partitioner
	 *            Partitioner to use. Pass <code>null</code> to use default
	 *            partitioner.
	 * @param quorumAddress
	 *            Distant cluster to write to; default is null for output to the
	 *            cluster that is designated in <code>hbase-site.xml</code>. Set
	 *            this String to the zookeeper ensemble of an alternate remote
	 *            cluster when you would have the reduce write a cluster that is
	 *            other than the default; e.g. copying tables between clusters,
	 *            the source would be designated by <code>hbase-site.xml</code>
	 *            and this param would have the ensemble address of the remote
	 *            cluster. The format to pass is particular. Pass
	 *            <code> &lt;hbase.zookeeper.quorum>:&lt;hbase.zookeeper.client.port>:&lt;zookeeper.znode.parent>
	 * </code> such as <code>server,server2,server3:2181:/hbase</code>.
	 * @param serverClass
	 *            redefined hbase.regionserver.class
	 * @param serverImpl
	 *            redefined hbase.regionserver.impl
	 * @throws IOException
	 *             When determining the region count fails.
	 */
	public static void setupGraphReducerJob(Query query,
			Class<? extends GraphReducer> reducer, Job job, Class partitioner,
			String quorumAddress, String serverClass, String serverImpl)
			throws IOException {
		setupGraphReducerJob(query, reducer, job, partitioner, quorumAddress,
				serverClass, serverImpl, true);
	}

	/**
	 * Use this before submitting a TableReduce job. It will appropriately set
	 * up the JobConf.
	 * 
	 * @param query
	 *            The output query.
	 * @param reducer
	 *            The reducer class to use.
	 * @param job
	 *            The current job to adjust. Make sure the passed job is
	 *            carrying all necessary HBase configuration.
	 * @param partitioner
	 *            Partitioner to use. Pass <code>null</code> to use default
	 *            partitioner.
	 * @param quorumAddress
	 *            Distant cluster to write to; default is null for output to the
	 *            cluster that is designated in <code>hbase-site.xml</code>. Set
	 *            this String to the zookeeper ensemble of an alternate remote
	 *            cluster when you would have the reduce write a cluster that is
	 *            other than the default; e.g. copying tables between clusters,
	 *            the source would be designated by <code>hbase-site.xml</code>
	 *            and this param would have the ensemble address of the remote
	 *            cluster. The format to pass is particular. Pass
	 *            <code> &lt;hbase.zookeeper.quorum>:&lt;hbase.zookeeper.client.port>:&lt;zookeeper.znode.parent>
	 * </code> such as <code>server,server2,server3:2181:/hbase</code>.
	 * @param serverClass
	 *            redefined hbase.regionserver.class
	 * @param serverImpl
	 *            redefined hbase.regionserver.impl
	 * @param addDependencyJars
	 *            upload HBase jars and jars for any of the configured job
	 *            classes via the distributed cache (tmpjars).
	 * @throws IOException
	 *             When determining the region count fails.
	 */
	public static void setupGraphReducerJob(Query query,
			Class<? extends GraphReducer> reducer, Job job, Class partitioner,
			String quorumAddress, String serverClass, String serverImpl,
			boolean addDependencyJars) throws IOException {

		Configuration conf = job.getConfiguration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
		
		PlasmaType type = getRootType(query);

		Where where = query.getModel().findWhereClause();
		SelectionCollector selectionCollector = null;
		if (where != null)
			selectionCollector = new SelectionCollector(query.getModel()
					.getSelectClause(), where, type);
		else
			selectionCollector = new SelectionCollector(query.getModel()
					.getSelectClause(), type);
		selectionCollector.setOnlyDeclaredProperties(false);
		// FIXME: generalize collectRowKeyProperties
		for (Type t : selectionCollector.getTypes())
			collectRowKeyProperties(selectionCollector, (PlasmaType) t);

		StateMarshalingContext marshallingContext = null;
		try {
			marshallingContext = new SimpleStateMarshallingContext(
					new StateNonValidatingDataBinding());
		} catch (JAXBException e) {
			throw new GraphServiceException(e);
		} catch (SAXException e) {
			throw new GraphServiceException(e);
		}

		DistributedGraphReader graphReader = new DistributedGraphReader(type,
				selectionCollector.getTypes(), marshallingContext);
		
		//job.setOutputFormatClass(GraphOutputFormat.class);
		if (reducer != null)
			job.setReducerClass(reducer);
		
		/*
		conf.set(TableOutputFormat.OUTPUT_TABLE, graphReader.getRootTableReader().getTableName());
		
		// If passed a quorum/ensemble address, pass it on to TableOutputFormat.
		if (quorumAddress != null) {
			// Calling this will validate the format
			ZKUtil.transformClusterKey(quorumAddress);
			conf.set(TableOutputFormat.QUORUM_ADDRESS, quorumAddress);
		}
		if (serverClass != null && serverImpl != null) {
			conf.set(TableOutputFormat.REGION_SERVER_CLASS, serverClass);
			conf.set(TableOutputFormat.REGION_SERVER_IMPL, serverImpl);
		}
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
		if (partitioner == HRegionPartitioner.class) {
			job.setPartitionerClass(HRegionPartitioner.class);
			HTable outputTable = new HTable(conf, graphReader.getRootTableReader().getTableName());
			int regions = outputTable.getRegionsInfo().size();
			if (job.getNumReduceTasks() > regions) {
				job.setNumReduceTasks(outputTable.getRegionsInfo().size());
			}
		} else if (partitioner != null) {
			job.setPartitionerClass(partitioner);
		}
		*/

		if (addDependencyJars) {
			addDependencyJars(job);
		}

		initCredentials(job);
	}

	/**
	 * Ensures that the given number of reduce tasks for the given job
	 * configuration does not exceed the number of regions for the given table.
	 * 
	 * @param table
	 *            The table to get the region count for.
	 * @param job
	 *            The current job to adjust.
	 * @throws IOException
	 *             When retrieving the table details fails.
	 */
	public static void limitNumReduceTasks(String table, Job job)
			throws IOException {
		HTable outputTable = new HTable(job.getConfiguration(), table);
		int regions = outputTable.getRegionLocations().size();
		if (job.getNumReduceTasks() > regions)
			job.setNumReduceTasks(regions);
	}

	/**
	 * Sets the number of reduce tasks for the given job configuration to the
	 * number of regions the given table has.
	 * 
	 * @param table
	 *            The table to get the region count for.
	 * @param job
	 *            The current job to adjust.
	 * @throws IOException
	 *             When retrieving the table details fails.
	 */
	public static void setNumReduceTasks(String table, Job job)
			throws IOException {
		HTable outputTable = new HTable(job.getConfiguration(), table);
		int regions = outputTable.getRegionLocations().size();
		job.setNumReduceTasks(regions);
	}

	/**
	 * Sets the number of rows to return and cache with each scanner iteration.
	 * Higher caching values will enable faster mapreduce jobs at the expense of
	 * requiring more heap to contain the cached rows.
	 * 
	 * @param job
	 *            The current job to adjust.
	 * @param batchSize
	 *            The number of rows to return in batch with each scanner
	 *            iteration.
	 */
	public static void setScannerCaching(Job job, int batchSize) {
		job.getConfiguration()
				.setInt("hbase.client.scanner.caching", batchSize);
	}

	/**
	 * Add the HBase dependency jars as well as jars for any of the configured
	 * job classes to the job configuration, so that JobClient will ship them to
	 * the cluster and add them to the DistributedCache.
	 */
	public static void addDependencyJars(Job job) throws IOException {
		try {
			addDependencyJars(
					job.getConfiguration(),
					org.apache.zookeeper.ZooKeeper.class,
					com.google.protobuf.Message.class,
					com.google.common.collect.ImmutableSet.class,
					org.apache.hadoop.hbase.util.Bytes.class, // one class from
																// hbase.jar
					job.getMapOutputKeyClass(), job.getMapOutputValueClass(),
					job.getInputFormatClass(), job.getOutputKeyClass(),
					job.getOutputValueClass(), job.getOutputFormatClass(),
					job.getPartitionerClass(), job.getCombinerClass());
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Add the jars containing the given classes to the job's configuration such
	 * that JobClient will ship them to the cluster and add them to the
	 * DistributedCache.
	 */
	public static void addDependencyJars(Configuration conf,
			Class<?>... classes) throws IOException {

		FileSystem localFs = FileSystem.getLocal(conf);
		Set<String> jars = new HashSet<String>();
		// Add jars that are already in the tmpjars variable
		jars.addAll(conf.getStringCollection("tmpjars"));

		// add jars as we find them to a map of contents jar name so that we can
		// avoid
		// creating new jars for classes that have already been packaged.
		Map<String, String> packagedClasses = new HashMap<String, String>();

		// Add jars containing the specified classes
		for (Class<?> clazz : classes) {
			if (clazz == null)
				continue;

			Path path = findOrCreateJar(clazz, localFs, packagedClasses);
			if (path == null) {
				LOG.warn("Could not find jar for class " + clazz
						+ " in order to ship it to the cluster.");
				continue;
			}
			if (!localFs.exists(path)) {
				LOG.warn("Could not validate jar file " + path + " for class "
						+ clazz);
				continue;
			}
			jars.add(path.toString());
		}
		if (jars.isEmpty())
			return;

		conf.set("tmpjars",
				StringUtils.arrayToString(jars.toArray(new String[0])));
	}

	/**
	 * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds
	 * the Jar for a class or creates it if it doesn't exist. If the class is in
	 * a directory in the classpath, it creates a Jar on the fly with the
	 * contents of the directory and returns the path to that Jar. If a Jar is
	 * created, it is created in the system temporary directory. Otherwise,
	 * returns an existing jar that contains a class of the same name. Maintains
	 * a mapping from jar contents to the tmp jar created.
	 * 
	 * @param my_class
	 *            the class to find.
	 * @param fs
	 *            the FileSystem with which to qualify the returned path.
	 * @param packagedClasses
	 *            a map of class name to path.
	 * @return a jar file that contains the class.
	 * @throws IOException
	 */
	private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
			Map<String, String> packagedClasses) throws IOException {
		// attempt to locate an existing jar for the class.
		String jar = findContainingJar(my_class, packagedClasses);
		if (null == jar || jar.isEmpty()) {
			jar = getJar(my_class);
			updateMap(jar, packagedClasses);
		}

		if (null == jar || jar.isEmpty()) {
			throw new IOException("Cannot locate resource for class "
					+ my_class.getName());
		}

		LOG.debug(String.format("For class %s, using jar %s",
				my_class.getName(), jar));
		return new Path(jar).makeQualified(fs);
	}

	/**
	 * Add entries to <code>packagedClasses</code> corresponding to class files
	 * contained in <code>jar</code>.
	 * 
	 * @param jar
	 *            The jar who's content to list.
	 * @param packagedClasses
	 *            map[class -> jar]
	 */
	private static void updateMap(String jar,
			Map<String, String> packagedClasses) throws IOException {
		ZipFile zip = null;
		try {
			zip = new ZipFile(jar);
			for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter
					.hasMoreElements();) {
				ZipEntry entry = iter.nextElement();
				if (entry.getName().endsWith("class")) {
					packagedClasses.put(entry.getName(), jar);
				}
			}
		} finally {
			if (null != zip)
				zip.close();
		}
	}

	/**
	 * Find a jar that contains a class of the same name, if any. It will return
	 * a jar file, even if that is not the first thing on the class path that
	 * has a class with the same name. Looks first on the classpath and then in
	 * the <code>packagedClasses</code> map.
	 * 
	 * @param my_class
	 *            the class to find.
	 * @return a jar file that contains the class, or null.
	 * @throws IOException
	 */
	private static String findContainingJar(Class<?> my_class,
			Map<String, String> packagedClasses) throws IOException {
		ClassLoader loader = my_class.getClassLoader();
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";

		// first search the classpath
		for (Enumeration<URL> itr = loader.getResources(class_file); itr
				.hasMoreElements();) {
			URL url = itr.nextElement();
			if ("jar".equals(url.getProtocol())) {
				String toReturn = url.getPath();
				if (toReturn.startsWith("file:")) {
					toReturn = toReturn.substring("file:".length());
				}
				// URLDecoder is a misnamed class, since it actually decodes
				// x-www-form-urlencoded MIME type rather than actual
				// URL encoding (which the file path has). Therefore it would
				// decode +s to ' 's which is incorrect (spaces are actually
				// either unencoded or encoded as "%20"). Replace +s first, so
				// that they are kept sacred during the decoding process.
				toReturn = toReturn.replaceAll("\\+", "%2B");
				toReturn = URLDecoder.decode(toReturn, "UTF-8");
				return toReturn.replaceAll("!.*$", "");
			}
		}

		// now look in any jars we've packaged using JarFinder. Returns null
		// when
		// no jar is found.
		return packagedClasses.get(class_file);
	}

	/**
	 * Invoke 'getJar' on a JarFinder implementation. Useful for some job
	 * configuration contexts (HBASE-8140) and also for testing on MRv2. First
	 * check if we have HADOOP-9426. Lacking that, fall back to the backport.
	 * 
	 * @param my_class
	 *            the class to find.
	 * @return a jar file that contains the class, or null.
	 */
	private static String getJar(Class<?> my_class) {
		String ret = null;
		String hadoopJarFinder = "org.apache.hadoop.util.JarFinder";
		Class<?> jarFinder = null;
		try {
			LOG.debug("Looking for " + hadoopJarFinder + ".");
			jarFinder = Class.forName(hadoopJarFinder);
			LOG.debug(hadoopJarFinder + " found.");
			Method getJar = jarFinder.getMethod("getJar", Class.class);
			ret = (String) getJar.invoke(null, my_class);
		} catch (ClassNotFoundException e) {
			LOG.debug("Using backported JarFinder.");
			ret = JarFinder.getJar(my_class);
		} catch (InvocationTargetException e) {
			// function was properly called, but threw it's own exception.
			// Unwrap it
			// and pass it on.
			throw new RuntimeException(e.getCause());
		} catch (Exception e) {
			// toss all other exceptions, related to reflection failure
			throw new RuntimeException("getJar invocation failed.", e);
		}

		return ret;
	}

	// FIXME: generalize
	private static Scan createScan(PartialRowKey partialRowKey,
			Filter columnFilter) {
		FilterList rootFilter = new FilterList(
				FilterList.Operator.MUST_PASS_ALL);
		rootFilter.addFilter(columnFilter);
		Scan scan = new Scan();
		scan.setFilter(rootFilter);
		scan.setStartRow(partialRowKey.getStartKey()); // inclusive
		scan.setStopRow(partialRowKey.getStopKey()); // exclusive
		LOG.info("using partial row key scan: (" + "start: '"
				+ Bytes.toString(scan.getStartRow()) + "' stop: '"
				+ Bytes.toString(scan.getStopRow()) + "')");
		return scan;
	}

	// FIXME: generalize
	private static Scan createScan(FuzzyRowKey fuzzyScan, Filter columnFilter)
			throws IOException {
		FilterList rootFilter = new FilterList(
				FilterList.Operator.MUST_PASS_ALL);
		rootFilter.addFilter(columnFilter);
		Scan scan = new Scan();
		scan.setFilter(rootFilter);
		Filter fuzzyFilter = fuzzyScan.getFilter();
		rootFilter.addFilter(fuzzyFilter);
		LOG.info("using fuzzy scan: " + FilterUtil.printFilterTree(fuzzyFilter));

		return scan;
	}

	private static void collectRowKeyProperties(SelectionCollector collector,
			PlasmaType type) {
		Config config = CloudGraphConfig.getInstance();
		DataGraphConfig graph = config.findDataGraph(type.getQualifiedName());
		if (graph != null) {
			UserDefinedRowKeyFieldConfig[] fields = new UserDefinedRowKeyFieldConfig[graph
					.getUserDefinedRowKeyFields().size()];
			graph.getUserDefinedRowKeyFields().toArray(fields);
			for (UserDefinedRowKeyFieldConfig field : fields) {
				List<Type> types = collector.addProperty(graph.getRootType(),
						field.getPropertyPath());
				for (Type nextType : types)
					collectRowKeyProperties(collector, (PlasmaType) nextType);
			}
		}
	}

	private static Query unmarshal(File queryFile) {
		try {
			PlasmaQueryDataBinding binding = new PlasmaQueryDataBinding(
					new DefaultValidationEventHandler());
			FileInputStream fis = new FileInputStream(queryFile);
			return (Query) binding.unmarshal(fis);
		} catch (JAXBException e1) {
			throw new GraphServiceException(e1);
		} catch (SAXException e1) {
			throw new GraphServiceException(e1);
		} catch (FileNotFoundException e) {
			throw new GraphServiceException(e);
		}
	}

	private static Query unmarshal(String xmlQuery) {
		try {
			PlasmaQueryDataBinding binding = new PlasmaQueryDataBinding(
					new DefaultValidationEventHandler());
			return (Query) binding.unmarshal(xmlQuery);
		} catch (JAXBException e1) {
			throw new GraphServiceException(e1);
		} catch (SAXException e1) {
			throw new GraphServiceException(e1);
		}
	}

	private static String marshal(Query query) {
		try {
			PlasmaQueryDataBinding binding = new PlasmaQueryDataBinding(
					new DefaultValidationEventHandler());
			return binding.marshal(query);
		} catch (JAXBException e1) {
			throw new GraphServiceException(e1);
		} catch (SAXException e1) {
			throw new GraphServiceException(e1);
		}
	}

}
