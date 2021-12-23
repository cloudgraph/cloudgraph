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
package org.cloudgraph.aerospike.mutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.aerospike.ext.Row;
import org.cloudgraph.aerospike.io.TableWriter;
import org.cloudgraph.store.key.KeyFieldOverflowException;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.sdo.core.SnapshotMap;

import com.aerospike.client.AerospikeException;

public class GraphMutationWriter {
  private static Log log = LogFactory.getLog(GraphMutationWriter.class);

  public void writeChanges(TableWriter[] tableWriters,
      Map<TableWriter, Map<String, Mutations>> mutations, SnapshotMap snapshotMap, String jobName)
      throws IOException {

    for (TableWriter tableWriter : tableWriters) {
      Map<String, Mutations> tableMutations = mutations.get(tableWriter);
      if (log.isDebugEnabled()) {
        if (tableWriter.hasConcurrentRows()
            && !tableWriter.getTableConfig().optimisticConcurrency()) {
          log.debug("commiting " + tableMutations.size() + " mutations to table: "
              + tableWriter.getTableConfig().getQualifiedPhysicalName()
              + " - ignoring concurrent processing for table");
        } else {
          log.debug("commiting " + tableMutations.size() + " mutations to table: "
              + tableWriter.getTableConfig().getQualifiedPhysicalName());
        }
      }
      List<Row> rows = getAllRows(tableMutations, tableWriter);

      // commit
      Object[] results = new Object[rows.size()];
      try {
        tableWriter.getTable().batch(rows, results);
      } catch (AerospikeException e) {
        // Caused by: com.aerospike.client.AerospikeException: Error
        // 21,1,0,30000,0,0,BB9B60C8B4C9A50 192.168.1.7 3000: Bin name length
        // greater than 14 characters or maximum bins exceeded
        if (e.getMessage() != null
            && e.getMessage().contains("Bin name length greater than 14 characters"))
          throw new KeyFieldOverflowException(e);
        throw new GraphServiceException(e);
      }
    }
  }

  private List<Row> getAllRows(Map<String, Mutations> tableMutations, TableWriter tableWriter) {
    List<Row> rows = new ArrayList<>();
    for (Mutations rowMutations : tableMutations.values())
      rows.addAll(rowMutations.getRows());
    if (log.isDebugEnabled()) {
      for (Row row : rows) {
        log.debug("commiting " + row.getClass().getSimpleName() + " mutation to table: "
            + tableWriter.getTableConfig().getQualifiedPhysicalName());
      }
    }
    return rows;
  }

}
