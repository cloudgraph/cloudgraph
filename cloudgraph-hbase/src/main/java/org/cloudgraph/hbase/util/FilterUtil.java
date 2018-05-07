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
package org.cloudgraph.hbase.util;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;

/**
 * HBase filter debugging utilities
 * 
 * @author Scott Cinnamond
 * @since 0.5
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
      buf.append(list.getOperator().name());
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
      if (filter instanceof CompareFilter) {
        CompareFilter compFilter = (CompareFilter) filter;
        buf.append(compFilter.getClass().getSimpleName() + ": ");
        buf.append(compFilter.getOperator().name());
        buf.append(" ");
        buf.append(compFilter.getComparator().getClass().getSimpleName());
        buf.append("('");
        buf.append(new String(compFilter.getComparator().getValue()));
        buf.append("')");
        buf.append(new String(compFilter.getComparator().getValue()));
      } else if (filter instanceof MultipleColumnPrefixFilter) {
        MultipleColumnPrefixFilter prefixFilter = (MultipleColumnPrefixFilter) filter;
        buf.append(filter.getClass().getSimpleName() + ": ");
        byte[][] prefixes = prefixFilter.getPrefix();
        for (int i = 0; i < prefixes.length; i++) {
          String prefix = new String(prefixes[i]);
          buf.append("\n");
          for (int j = 0; j < level + 1; j++)
            buf.append("\t");
          buf.append(prefix);
        }
      } else if (filter instanceof RandomRowFilter) {
        RandomRowFilter rowFilter = (RandomRowFilter) filter;
        buf.append(filter.getClass().getSimpleName() + ": ");
        buf.append(rowFilter.getChance());
      } else {
        buf.append(filter.toString());
      }
    }
  }

}
