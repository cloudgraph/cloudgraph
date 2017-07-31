package org.cloudgraph.hbase.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.cloudgraph.job.JobSetup;
import org.plasma.query.Query;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

public class GraphSparkSetup extends JobSetup {

  public static void setupJob(Query query, Job job) {
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

    PlasmaType type = getRootType(query);

    Where where = query.getModel().findWhereClause();
    SelectionCollector selectionCollector = null;
    if (where != null)
      selectionCollector = new SelectionCollector(query.getModel().getSelectClause(), where, type);
    else
      selectionCollector = new SelectionCollector(query.getModel().getSelectClause(), type);
    selectionCollector.setOnlyDeclaredProperties(false);

  }
}
