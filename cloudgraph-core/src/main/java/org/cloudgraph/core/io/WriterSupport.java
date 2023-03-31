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
package org.cloudgraph.core.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.key.CompositeRowKeyFactory;
import org.cloudgraph.core.io.GraphRowWriter;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreDataObject;

import commonj.sdo.DataObject;

public abstract class WriterSupport {
  private static Log log = LogFactory.getLog(WriterSupport.class);
  protected Map<DataObject, RowWriter> rowWriterMap = new HashMap<DataObject, RowWriter>();
  protected ServiceContext serviceContext;

  @SuppressWarnings("unused")
  private WriterSupport() {
  }

  protected WriterSupport(ServiceContext mappingContext) {
    this.serviceContext = mappingContext;
  }

  /**
   * Finds and return the row writer associated with the container for the given
   * target data object
   * 
   * @param target
   *          the target data object
   * @return the row writer associated with the container for the given target
   *         data object
   * @throws OperationException
   *           if no row writer found
   */
  protected RowWriter getContainerRowWriter(DataObject target) {
    RowWriter result = findContainerRowWriter(target);
    if (result == null)
      throw new OperationException("no row writer associated with data object, "
          + String.valueOf(target) + ", or its containment ancestry");
    return result;
  }

  /**
   * Finds and return the row writer associated with the given target data
   * object
   * 
   * @param target
   *          the target data object
   * @return the row writer associated with the given target data object, or
   *         null if no writer found
   */
  protected RowWriter findContainerRowWriter(DataObject target) {
    RowWriter result = null;
    DataObject container = target.getContainer();
    while (container != null) {
      if (log.isDebugEnabled())
        log.debug("checking " + container + " container for " + target);
      result = this.rowWriterMap.get(container);
      if (result != null)
        return result;
      container = container.getContainer();
    }
    return null;
  }

  /**
   * Traverses the container links for the given data object returning the
   * ancestry as an ordered list.
   * 
   * @param target
   * @return
   */
  protected List<DataObject> getContainerAncestry(DataObject target) {
    List<DataObject> result = new ArrayList<DataObject>();
    DataObject source = target.getContainer();
    while (source != null) {
      result.add(source);
      source = source.getContainer();
    }
    return result;
  }

  /**
   * Creates and returns a new row writer for the given (row root) data object.
   * If an existing row key is set into the instance property collection for the
   * (row root) data object, the existing row key is used, otherwise a row key
   * is generated. For objects flagged as deleted in the SDO change summary,
   * re-generating a row key may not be possible, as for deleted operations SDO
   * requires the removal of all properties (other than read-only) for all
   * deleted data objects. If such removed properties are required for a row
   * key, this makes re-generation for the row key impossible, and the key
   * generator will throw an error.
   * 
   * @param tableWriter
   *          the table writer
   * @param target
   *          the target (row root) data object
   * @return the new row writer
   * @throws IOException
   */
  protected RowWriter createRowWriter(TableWriter tableWriter, DataObject target)
      throws IOException {
    CoreDataObject coreObject = (CoreDataObject) target;
    byte[] rowKey = (byte[]) coreObject.getValueObject().get(CloudGraphConstants.ROW_KEY);
    if (rowKey == null) {
      CompositeRowKeyFactory rowKeyGen = new CompositeRowKeyFactory((PlasmaType) target.getType(),
          this.serviceContext);
      rowKey = rowKeyGen.createRowKeyBytes(target);
      coreObject.getValueObject().put(CloudGraphConstants.ROW_KEY, rowKey);
      if (target.getDataGraph().getRootObject().equals(target)) // graph
        // root
        // object
        ((PlasmaDataGraph) target.getDataGraph()).setId(rowKey); // FIXME:
      // snapshot
      // map
      // for
      // this?
    }
    RowWriter rowWriter = this.createRowWriter(tableWriter, target, rowKey);

    return rowWriter;
  }

  /**
   * Creates and returns a new row writer for the given (row root) data object.
   * 
   * @param tableWriter
   *          the table context
   * @param dataObject
   *          the (row root) data object
   * @param rowKey
   *          the existing row key
   * @return the new row writer
   * @throws IOException
   */
  protected RowWriter createRowWriter(TableWriter tableWriter, DataObject dataObject, byte[] rowKey)
      throws IOException {
    RowWriter rowContext = new GraphRowWriter(rowKey, dataObject, tableWriter, this.serviceContext);

    UUID uuid = ((PlasmaDataObject) dataObject).getUUID();
    tableWriter.addRowWriter(uuid, rowContext);

    return rowContext;
  }

  /**
   * Creates adds and returns a new row writer for the given (row root) data
   * object. If an existing row key is set into the instance property collection
   * for the (row root) data object, the existing row key is used, otherwise a
   * row key is generated. For objects flagged as deleted in the SDO change
   * summary, re-generating a row key may not be possible, as for deleted
   * operations SDO requires the removal of all properties (other than
   * read-only) for all deleted data objects. If such removed properties are
   * required for a row key, this makes re-generation for the row key
   * impossible, and the key generator will throw an error.
   * 
   * @param dataObject
   *          the target (row root) data object
   * @param tableWriter
   *          the table context
   * @return the new row writer
   * @throws IOException
   */
  protected RowWriter addRowWriter(DataObject dataObject, TableWriter tableWriter)
      throws IOException {
    byte[] rowKey = null;
    CoreDataObject coreObject = (CoreDataObject) dataObject;
    rowKey = (byte[]) coreObject.getValueObject().get(CloudGraphConstants.ROW_KEY);
    if (rowKey == null) {
      CompositeRowKeyFactory rowKeyGen = new CompositeRowKeyFactory(
          (PlasmaType) dataObject.getType(), this.serviceContext);
      rowKey = rowKeyGen.createRowKeyBytes(dataObject);
      coreObject.getValueObject().put(CloudGraphConstants.ROW_KEY, rowKey);
    }
    RowWriter rowContext = new GraphRowWriter(rowKey, dataObject, tableWriter, this.serviceContext);
    UUID uuid = ((PlasmaDataObject) dataObject).getUUID();
    tableWriter.addRowWriter(uuid, rowContext);
    return rowContext;
  }
}
