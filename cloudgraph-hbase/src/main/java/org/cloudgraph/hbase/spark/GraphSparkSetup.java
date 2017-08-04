/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
