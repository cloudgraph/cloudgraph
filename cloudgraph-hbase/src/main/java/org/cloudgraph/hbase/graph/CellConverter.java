package org.cloudgraph.hbase.graph;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.io.CellValues;
import org.cloudgraph.hbase.io.DefaultEdgeOperation;
import org.cloudgraph.hbase.io.GraphRowWriter;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.KeyValue;
import org.plasma.sdo.Key;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.profile.KeyStructure;

public class CellConverter {
  private static Log log = LogFactory.getLog(CellConverter.class);
  private PlasmaType rootType;
  private TableConfig rootTableConfig;
  private GraphColumnKeyFactory keyFactory;
  private HBaseDataConverter hbaseConverter = HBaseDataConverter.INSTANCE;

  public CellConverter(PlasmaType rootType, TableConfig rootTableConfig) {
    super();
    this.rootType = rootType;
    this.rootTableConfig = rootTableConfig;
    this.keyFactory = new CompositeColumnKeyFactory(rootType);
  }

  public CellValues convert(String rowKey, Collection<KeyValue> values) {
    CellValues result = new CellValues(rowKey);

    byte[] typeQual = keyFactory.createColumnKey(this.rootType, EntityMetaKey.TYPE);
    byte[] typeValue = GraphRowWriter.encode(this.rootType);
    result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), typeQual, typeValue);

    for (KeyValue keyValue : values) {
      if (keyValue.getProp().getContainingType().equals(this.rootType)) {
        Key key = keyValue.getProp().getKey();
        if (key != null
            && key.getStructure() != null
            && KeyStructure.valueOf(key.getStructure().name()).ordinal() == KeyStructure.uuid
                .ordinal()) {
          byte[] qual = keyFactory.createColumnKey(this.rootType, EntityMetaKey.UUID);
          byte[] value = hbaseConverter.toBytes(keyValue.getProp(), keyValue.getValue());
          result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), qual, value);
        } else {
          byte[] qual = keyFactory.createColumnKey(this.rootType, keyValue.getProp());
          byte[] value = hbaseConverter.toBytes(keyValue.getProp(), keyValue.getValue());
          result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), qual, value);
        }
      } else {
        log.warn("converting non-root value, " + keyValue);
        byte[] qual = keyFactory.createColumnKey((PlasmaType) keyValue.getProp()
            .getContainingType(), keyValue.getProp());
        byte[] value = hbaseConverter.toBytes(keyValue.getProp(), keyValue.getValue());
        result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), qual, value);
      }
    }
    return result;

  }

}
