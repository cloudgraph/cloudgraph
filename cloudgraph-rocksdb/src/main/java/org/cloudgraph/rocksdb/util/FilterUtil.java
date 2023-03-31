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
package org.cloudgraph.rocksdb.util;

import java.io.IOException;

import org.cloudgraph.rocksdb.filter.Filter;
import org.cloudgraph.rocksdb.filter.FilterList;
import org.cloudgraph.rocksdb.filter.MultiColumnCompareFilter;
import org.cloudgraph.rocksdb.filter.MultiColumnPrefixFilter;

/**
 * RocksDB filter debugging utilities
 * 
 * @author Scott Cinnamond
 * @since 2.0.0
 */
public class FilterUtil {

  public static String printFilterTree(Filter filter) throws IOException {
    StringBuilder buf = new StringBuilder();
    printFilterTree(filter, buf, 0);
    return buf.toString();
  }

  public static void printFilterTree(Filter filter, StringBuilder buf, int level)
      throws IOException {
    if (filter instanceof FilterList) {
      buf.append("\n");
      for (int i = 0; i < level; i++)
        buf.append("\t");
      FilterList list = (FilterList) filter;
      buf.append("[LIST:");
      buf.append(list.getOper().name());
      for (Filter child : list.getFilters()) {
        printFilterTree(child, buf, level + 1);
      }
      buf.append("\n");
      for (int i = 0; i < level; i++)
        buf.append("\t");
      buf.append("]");
    } else {
      buf.append("\n");
      for (int i = 0; i < level; i++)
        buf.append("\t");
      if (filter instanceof MultiColumnCompareFilter) {
        MultiColumnCompareFilter compFilter = (MultiColumnCompareFilter) filter;
        buf.append(compFilter.getClass().getSimpleName() + ": ");
        buf.append(compFilter);
      } else if (filter instanceof MultiColumnPrefixFilter) {
        MultiColumnPrefixFilter prefixFilter = (MultiColumnPrefixFilter) filter;
        buf.append(prefixFilter.getClass().getSimpleName() + ": ");
        buf.append(prefixFilter);
      } else {
        buf.append(filter.toString());
      }
    }
  }

}
