package org.cloudgraph.core.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.Table;
import org.cloudgraph.store.mapping.TableMapping;

public abstract class DefaultClientFactory {
  private static Log log = LogFactory.getLog(DefaultClientFactory.class);

  protected String createPhysicalNamespaceQualifiedPhysicalName(String physicalNameDelimiter,
      TableMapping table, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    name.append(this.createPhysicalNamespace(physicalNameDelimiter, table, context));
    name.append(physicalNameDelimiter);
    name.append(table.getTable().getName());
    return name.toString();
  }

  protected String createPhysicalNamespace(String physicalNameDelimiter, TableMapping table,
      StoreMappingContext context) {
    StringBuilder namespace = new StringBuilder();

    // replace namespace logical delimiters with physical
    String delim = physicalNameDelimiter;
    if (context.hasTablePhysicalNamespaceDelim()) {
      delim = context.getTablePhysicalNamespaceDelim();
    }
    String physicalNamespace = table.getTable().getNamespace()
        .replaceAll(TableMapping.TABLE_LOGICAL_NAME_DELIM, delim);

    String rootPath = StoreMapping.getInstance().tableNamespaceRoot();
    // prepend the root path if exists
    if (rootPath != null) {
      namespace.append(rootPath);
      namespace.append(delim);
    }
    // prepend the volume name from either the
    // table config or volume name or cont4xt volume name
    String volumeName = findVolumeName(table.getTable(), context);
    if (volumeName != null && !physicalNamespace.startsWith(volumeName)) {
      namespace.append(volumeName);
      namespace.append(delim);
    }

    namespace.append(physicalNamespace);

    return namespace.toString();
  }

  private String findVolumeName(Table table, StoreMappingContext context) {
    if (table.getTableVolumeName() != null) {
      if (context != null && context.hasTableVolumeName()
          && !table.getTableVolumeName().equals(context.getTableVolumeName())) {
        log.warn("overriding table volumme '" + table.getTableVolumeName()
            + "' with context volue '" + context.getTableVolumeName() + "'");
        return context.getTableVolumeName();
      } else {
        return table.getTableVolumeName();

      }
    } else if (context != null && context.hasTableVolumeName()) {
      return context.getTableVolumeName();
    }
    return null;
  }

}
