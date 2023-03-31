package org.cloudgraph.rocksdb.proto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.ext.Column;
import org.cloudgraph.rocksdb.proto.TupleProto.Cell;
import org.cloudgraph.rocksdb.proto.TupleProto.Tuple;
import org.cloudgraph.store.service.GraphServiceException;

import com.google.protobuf.ByteString;

public class TupleCodec {
  private static Log log = LogFactory.getLog(TupleCodec.class);

  public static byte[] encodeRow(Column[] columns) {
    Tuple.Builder RowBuilder = Tuple.newBuilder();
    for (Column column : columns) {
      Cell.Builder cellBuilder = Cell.newBuilder();
      cellBuilder.setName(ByteString.copyFrom(column.getNameBytes()));
      cellBuilder.setValue(ByteString.copyFrom(column.getData()));
      RowBuilder.addCell(cellBuilder);
    }
    Tuple tuple = RowBuilder.build();
    // if (log.isTraceEnabled())
    // log.trace("encoded: " + tuple);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] bytes = null;
    try {
      tuple.writeDelimitedTo(baos);
      baos.flush();
      bytes = baos.toByteArray();
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } finally {
      try {
        baos.close();
      } catch (IOException e) {
      }
    }
    return bytes;
  }

  public static Column[] decodeRow(byte[] data) {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    try {
      Tuple.Builder rowBuilder = Tuple.newBuilder();
      rowBuilder.mergeDelimitedFrom(bais);
      Tuple tuple = rowBuilder.build();
      // if (log.isTraceEnabled())
      // log.trace("decoded: " + tuple);
      Column[] result = new Column[tuple.getCellCount()];
      int i = 0;
      for (Cell cell : tuple.getCellList()) {
        result[i] = new Column(cell.getName().toStringUtf8(), cell.getValue().toByteArray());
        i++;
      }
      return result;
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } finally {
      try {
        bais.close();
      } catch (IOException e) {
      }
    }
  }
}
