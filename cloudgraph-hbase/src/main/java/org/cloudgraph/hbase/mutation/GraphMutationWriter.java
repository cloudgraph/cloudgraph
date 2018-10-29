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
package org.cloudgraph.hbase.mutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.Pair;
import org.cloudgraph.hbase.io.RowWriter;
import org.cloudgraph.hbase.io.TableWriter;
import org.cloudgraph.store.service.GraphServiceException;
import org.cloudgraph.store.service.OptimisticConcurrencyException;
import org.plasma.sdo.Increment;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.Concurrent;
import org.plasma.sdo.core.SnapshotMap;

public class GraphMutationWriter {
  private static Log log = LogFactory.getLog(GraphMutationWriter.class);

  public void writeChanges(TableWriter[] tableWriters,
      Map<TableWriter, Map<String, Mutations>> mutations, SnapshotMap snapshotMap, String jobName)
      throws IOException {

    for (TableWriter tableWriter : tableWriters) {
      Map<String, Mutations> tableMutations = mutations.get(tableWriter);
      if (log.isDebugEnabled())
        log.debug("commiting " + tableMutations.size() + " mutations to table: "
            + tableWriter.getTableConfig().getName());
      
      if (!tableWriter.hasConcurrentRows()) { 
        List<Row> rows = getAllRows(tableMutations, tableWriter);
        
        // commit
        Object[] results = new Object[rows.size()];
        try {
          tableWriter.getTable().batch(rows, results);
        } catch (InterruptedException e) {
          throw new GraphServiceException(e);
        }
        
        // read results and map back to client if applicable
        mapResults(results, tableMutations, snapshotMap, jobName);
      }
      else { 
        // attempt commit of all concurrent rows
        // before any others
        List<Row> rows = getConcurrentRows(tableMutations, tableWriter);
        
        // map list of row mutations to common rows
        Map<String, List<Row>> tableRowMap = new HashMap<>();
        for (Row row : rows) {
          List<Row> rowMutations = tableRowMap.get(Bytes.toString(row.getRow()));
          if (rowMutations == null) {
            rowMutations = new ArrayList<>();
            tableRowMap.put(Bytes.toString(row.getRow()), rowMutations);
          }
          rowMutations.add(row);  
        }
        
        for (List<Row> rowMutations : tableRowMap.values()) {
          RowMutations checkedMutations = new RowMutations(null);
          Mutation commitRow = null;
          for (Row row : rowMutations) {
            if (commitRow == null)
              commitRow = (Mutation)row;
            if (Put.class.isInstance(row))
              checkedMutations.add(Put.class.cast(row));
            else if (Delete.class.isInstance(row))
              checkedMutations.add(Delete.class.cast(row));
            else
              throw new GraphServiceException("unexpected mutation class for concurrent row, " + row.getClass());
          }
          
          byte[] checkFamBytes = commitRow.getAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_FAM_BYTES);
          if (checkFamBytes == null)
            throw new GraphServiceException("expected row attribute, " + RowWriter.ROW_ATTR_NAME_CONCURRENT_FAM_BYTES);
          byte[] checkQualBytes = commitRow.getAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_QUAL_BYTES);
          if (checkQualBytes == null)
            throw new GraphServiceException("expected row attribute, " + RowWriter.ROW_ATTR_NAME_CONCURRENT_QUAL_BYTES);
          byte[] checkValueBytes = commitRow.getAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_VALUE_BYTES);
          if (checkValueBytes == null)
            throw new GraphServiceException("expected row attribute, " + RowWriter.ROW_ATTR_NAME_CONCURRENT_VALUE_BYTES);
         
          if (!tableWriter.getTable().checkAndMutate(commitRow.getRow(), checkFamBytes, checkQualBytes, CompareOp.EQUAL, checkValueBytes, checkedMutations))
            throw new OptimisticConcurrencyException("concurrency failure detected - there were "+
                "one or more intervening updates for entity");                             
        }
        
      }
      
    }
  }
  
  private void mapResults(Object[] results, Map<String, Mutations> tableMutations, SnapshotMap snapshotMap, String jobName)
  {
    for (int i = 0; i < results.length; i++) {
      if (results[i] == null) {
        log.error("batch action (" + i + ") for job '" + jobName + "' failed with null result");
      } else {
        if (Result.class.isInstance(results[i])) {
          if (log.isDebugEnabled())
            log.debug("batch action (" + i + ") for job '" + jobName + "' succeeded with "
                + String.valueOf(results[i]) + " result");
          Result result = (Result) results[i];
          Mutations rowMutations = tableMutations.get(Bytes.toString(result.getRow()));
          if (result.rawCells() != null)
            for (Cell cell : result.rawCells()) {

              String fam = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
                  cell.getFamilyLength());
              String qual = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                  cell.getQualifierLength());

              Pair<PlasmaDataObject, PlasmaProperty> pair = rowMutations.get(Bytes.toBytes(fam),
                  Bytes.toBytes(qual));

              // see if we need to propagate results back to caller
              Increment increment = pair.getRight().getIncrement();
              if (increment != null) {
                Long value = Bytes.toLong(cell.getValueArray(), cell.getValueOffset(),
                    cell.getValueLength());
                snapshotMap.put(pair.getLeft().getUUID(),
                    new PropertyPair(pair.getRight(), value));
              }              
            }
        } else {
          if (log.isDebugEnabled())
            log.debug("batch action (" + i + ") for job '" + jobName + "' succeeded with "
                + String.valueOf(results[i]) + " result");
        }
      }
    }    
  }
  
  private List<Row> getAllRows(Map<String, Mutations> tableMutations, TableWriter tableWriter)
  {
    List<Row> rows = new ArrayList<>();
    for (Mutations rowMutations : tableMutations.values())
      rows.addAll(rowMutations.getRows());
    if (log.isDebugEnabled()) {
      for (Row row : rows) {
        log.debug("commiting " + row.getClass().getSimpleName() + " mutation to table: "
            + tableWriter.getTableConfig().getName());
        debugRowValues(row);
      }
    }
    return rows;
  }
  
  private List<Row> getConcurrentRows(Map<String, Mutations> tableMutations, TableWriter tableWriter)
  {
    List<Row> rows = new ArrayList<>();
    for (Mutations rowMutations : tableMutations.values())
      for (Row row : rowMutations.getRows()) {
        //FIXME: get rid of cast
        byte[] concurrent = Mutation.class.cast(row).getAttribute(RowWriter.ROW_ATTR_NAME_IS_CONCURRENT_BOOL);
        if (concurrent != null && Bytes.toBoolean(concurrent)) {
          rows.add(row);
        }
      }
    if (log.isDebugEnabled()) {
      for (Row row : rows) {
        log.debug("commiting concurrent " + row.getClass().getSimpleName() + " mutation to table: "
            + tableWriter.getTableConfig().getName());
        debugRowValues(row);
      }
    }
    return rows;
  }
  

  private void debugRowValues(Row row) {
    if (row instanceof Mutation) {
      Mutation mutation = (Mutation) row;
      NavigableMap<byte[], List<Cell>> map = mutation.getFamilyCellMap();
      StringBuilder buf = new StringBuilder();
      Iterator<byte[]> iter = map.keySet().iterator();
      buf.append("[");
      int i = 0;
      while (iter.hasNext()) {
        if (i > 0)
          buf.append(", ");
        byte[] family = iter.next();
        List<Cell> list = map.get(family);
        for (Cell cell : list) {
          buf.append(Bytes.toString(family));
          buf.append(":");
          byte[] qual = CellUtil.cloneQualifier(cell);
          buf.append(Bytes.toString(qual));
          buf.append("=");
          byte[] value = CellUtil.cloneValue(cell);
          buf.append(Bytes.toString(value));
        }
      }
      buf.append("]");
      log.debug("values: " + buf.toString());
    }
  }

}
