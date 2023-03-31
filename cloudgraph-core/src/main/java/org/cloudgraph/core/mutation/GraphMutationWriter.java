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
package org.cloudgraph.core.mutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.ServiceContext;
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Mutation;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Row;
//import org.apache.hadoop.hbase.client.RowMutations;
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
//import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.CellUtil;
import org.cloudgraph.core.client.CompareOper;
import org.cloudgraph.core.client.Delete;
import org.cloudgraph.core.client.Mutation;
import org.cloudgraph.core.client.Put;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.Row;
import org.cloudgraph.core.client.RowMutations;
import org.cloudgraph.common.Pair;
import org.cloudgraph.core.io.RowWriter;
import org.cloudgraph.core.io.TableWriter;
import org.cloudgraph.store.service.GraphServiceException;
import org.cloudgraph.store.service.OptimisticConcurrencyException;
import org.plasma.sdo.Increment;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.core.SnapshotMap;

public class GraphMutationWriter {
  private static Log log = LogFactory.getLog(GraphMutationWriter.class);

  private ServiceContext serviceContext;

  @SuppressWarnings("unused")
  private GraphMutationWriter() {
  }

  public GraphMutationWriter(ServiceContext serviceContext) {
    super();
    this.serviceContext = serviceContext;
  }

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

      if (!tableWriter.hasConcurrentRows() || !tableWriter.getTableConfig().optimisticConcurrency()) {
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
      } else {
        // first attempt commit of all concurrent rows
        // before any others
        List<Row> rows = getConcurrentRows(tableMutations, tableWriter);
        Map<String, List<Row>> tableRowMap = collectRowMutations(rows);
        for (List<Row> rowMutations : tableRowMap.values()) {
          commitConcurrentRow(rowMutations, tableWriter);
        }

        // after every concurrent row succeeds, commit any remaining
        // non-concurrent rows
        rows = getNonConcurrentRows(tableMutations, tableWriter);

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

    }
  }

  /**
   * Commits a list of mutations against a single row using attributes
   * previously valued on the row to set up a checkAndMutate() operation.
   * 
   * @param rowMutations
   *          the row mutations
   * @param tableWriter
   *          the table writer
   * @throws IOException
   * @throws OptimisticConcurrencyException
   *           if an optimistic concurrency error is detected.
   */
  private void commitConcurrentRow(List<Row> rowMutations, TableWriter tableWriter)
      throws IOException, OptimisticConcurrencyException {
    Mutation commitRow = (Mutation) rowMutations.get(0); // all should be same
                                                         // row
    RowMutations checkedMutations = this.serviceContext.getClientFactory().createRowMutations(
        commitRow.getRow());

    for (Row row : rowMutations) {
      if (Put.class.isInstance(row))
        checkedMutations.add(Put.class.cast(row));
      else if (Delete.class.isInstance(row))
        checkedMutations.add(Delete.class.cast(row));
      else
        throw new GraphServiceException("unexpected mutation class for concurrent row, "
            + row.getClass());
      if (log.isDebugEnabled()) {
        log.debug("commiting concurrent " + row.getClass().getSimpleName() + " mutation to table: "
            + tableWriter.getTableConfig().getQualifiedPhysicalName());
        debugRowValues(row);
      }
    }

    byte[] checkFamBytes = commitRow.getAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_FAM_BYTES);
    if (checkFamBytes == null)
      throw new GraphServiceException("expected row attribute, "
          + RowWriter.ROW_ATTR_NAME_CONCURRENT_FAM_BYTES);
    byte[] checkQualBytes = commitRow.getAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_QUAL_BYTES);
    if (checkQualBytes == null)
      throw new GraphServiceException("expected row attribute, "
          + RowWriter.ROW_ATTR_NAME_CONCURRENT_QUAL_BYTES);
    byte[] checkValueBytes = commitRow.getAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_VALUE_BYTES);
    if (checkValueBytes == null)
      throw new GraphServiceException("expected row attribute, "
          + RowWriter.ROW_ATTR_NAME_CONCURRENT_VALUE_BYTES);

    if (!tableWriter.getTable().checkAndMutate(commitRow.getRow(), checkFamBytes, checkQualBytes,
        CompareOper.EQUAL, checkValueBytes, checkedMutations))
      throw new OptimisticConcurrencyException("concurrency failure detected - "
          + "one or more intervening updates have occurred on row '"
          + Bytes.toString(commitRow.getRow()) + "' in table, " + tableWriter.getTable().getName());
  }

  /**
   * Map list of row mutations to common rows
   * 
   * @param rows
   * @return
   */
  private Map<String, List<Row>> collectRowMutations(List<Row> rows) {
    Map<String, List<Row>> tableRowMap = new HashMap<>();
    for (Row row : rows) {
      List<Row> rowMutations = tableRowMap.get(Bytes.toString(row.getRow()));
      if (rowMutations == null) {
        rowMutations = new ArrayList<>();
        tableRowMap.put(Bytes.toString(row.getRow()), rowMutations);
      }
      rowMutations.add(row);
    }
    return tableRowMap;
  }

  private void mapResults(Object[] results, Map<String, Mutations> tableMutations,
      SnapshotMap snapshotMap, String jobName) {
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
                snapshotMap.put(pair.getLeft().getUUID(), new PropertyPair(pair.getRight(), value));
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

  private List<Row> getAllRows(Map<String, Mutations> tableMutations, TableWriter tableWriter) {
    List<Row> rows = new ArrayList<>();
    for (Mutations rowMutations : tableMutations.values())
      rows.addAll(rowMutations.getRows());
    if (log.isDebugEnabled()) {
      for (Row row : rows) {
        log.debug("commiting " + row.getClass().getSimpleName() + " mutation to table: "
            + tableWriter.getTableConfig().getQualifiedPhysicalName());
        debugRowValues(row);
      }
    }
    return rows;
  }

  private List<Row> getConcurrentRows(Map<String, Mutations> tableMutations, TableWriter tableWriter) {
    List<Row> rows = new ArrayList<>();
    for (Mutations rowMutations : tableMutations.values())
      for (Row row : rowMutations.getRows()) {
        // FIXME: get rid of cast
        byte[] concurrent = Mutation.class.cast(row).getAttribute(
            RowWriter.ROW_ATTR_NAME_IS_CONCURRENT_BOOL);
        if (concurrent != null && Bytes.toBoolean(concurrent)) {
          rows.add(row);
        }
      }
    return rows;
  }

  private List<Row> getNonConcurrentRows(Map<String, Mutations> tableMutations,
      TableWriter tableWriter) {
    List<Row> rows = new ArrayList<>();
    for (Mutations rowMutations : tableMutations.values())
      for (Row row : rowMutations.getRows()) {
        // FIXME: get rid of cast
        byte[] concurrent = Mutation.class.cast(row).getAttribute(
            RowWriter.ROW_ATTR_NAME_IS_CONCURRENT_BOOL);
        if (concurrent == null || !Bytes.toBoolean(concurrent)) {
          rows.add(row);
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
          byte[] qual = this.serviceContext.getClientFactory().getCellUtil().cloneQualifier(cell);
          buf.append(Bytes.toString(qual));
          buf.append("=");
          byte[] value = this.serviceContext.getClientFactory().getCellUtil().cloneValue(cell);
          buf.append(Bytes.toString(value));
        }
      }
      buf.append("]");
      log.debug("values: " + buf.toString());
    }
  }

}
