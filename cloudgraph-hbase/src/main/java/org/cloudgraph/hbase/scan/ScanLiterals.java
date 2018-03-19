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
package org.cloudgraph.hbase.scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.plasma.query.Wildcard;
import org.plasma.query.model.RelationalOperatorName;

/**
 * A collection of scan literals which provides various accessor methods which
 * indicate the applicability of the scan literal collection under various scan
 * operations. Given that a query may represent any number of scans or gets,
 * clients should ensure that the collection is populated with literals
 * applicable for its query expression context.
 * 
 * @see ScanLiteral
 * @author Scott Cinnamond
 * @since 0.5
 */
public class ScanLiterals {
  private static Log log = LogFactory.getLog(ScanLiterals.class);
  private Map<Integer, List<ScanLiteral>> literalMap = new HashMap<Integer, List<ScanLiteral>>();
  private List<ScanLiteral> literalList = new ArrayList<ScanLiteral>();
  private boolean hasWildcardLiterals = false;
  private boolean hasMultipleWildcardLiterals = false;
  private boolean hasOtherThanSingleTrailingWildcards = false;
  private boolean hasOnlyEqualityRelationalOperators = true;
  private Map<DataGraphMapping, Boolean> hasContiguousPartialKeyScanFieldValuesMap;
  private Map<DataGraphMapping, Boolean> hasContiguousKeyFieldValuesMap;

  public ScanLiterals() {
  }

  public List<ScanLiteral> getLiterals() {
    return literalList;
  }

  public List<ScanLiteral> getLiterals(DataRowKeyFieldMapping fieldConfig) {
    return literalMap.get(fieldConfig.getSequenceNum());
  }

  public int size() {
    return this.literalList.size();
  }

  public void addLiteral(ScanLiteral scanLiteral) {
    if (scanLiteral instanceof WildcardStringLiteral) {
      if (this.hasWildcardLiterals)
        this.hasMultipleWildcardLiterals = true;
      this.hasWildcardLiterals = true;

      WildcardStringLiteral wildcardStringLiteral = (WildcardStringLiteral) scanLiteral;
      String content = wildcardStringLiteral.getContent().trim();
      if (!content.endsWith(Wildcard.WILDCARD_CHAR)) {
        this.hasOtherThanSingleTrailingWildcards = true;
      } else {
        // it has another wildcard preceding the trailing one
        if (content.indexOf(Wildcard.WILDCARD_CHAR) < content.length() - 1) {
          this.hasOtherThanSingleTrailingWildcards = true;
        }
      }
    }

    RelationalOperatorName oper = scanLiteral.getRelationalOperator();
    if (oper != null) {
      switch (oper) {
      case EQUALS:
        break;
      default:
        this.hasOnlyEqualityRelationalOperators = false;
      }
    }

    DataRowKeyFieldMapping fieldConfig = scanLiteral.getFieldConfig();
    List<ScanLiteral> list = this.literalMap.get(fieldConfig.getSequenceNum());
    if (list == null) {
      list = new ArrayList<ScanLiteral>(4);
      this.literalMap.put(fieldConfig.getSequenceNum(), list);
    }
    list.add(scanLiteral);
    this.literalList.add(scanLiteral);
  }

  /**
   * Returns true if this set of literals can support a partial row key scan for
   * the given graph
   * 
   * @param graph
   *          the graph
   * @return true if this set of literals can support a partial row key scan for
   *         the given graph
   */
  public boolean supportPartialRowKeyScan(DataGraphMapping graph) {
    if (this.hasMultipleWildcardLiterals || this.hasOtherThanSingleTrailingWildcards)
      return false;

    // ensure if there is a wildcard literal that its the last literal
    // in terms of sequence within the row key definition
    if (this.hasWildcardLiterals) {
      int maxLiteralSeq = 0;
      int wildcardLiteralSeq = 0;
      for (ScanLiteral literal : literalList) {
        if (literal.getFieldConfig().getSeqNum() > maxLiteralSeq)
          maxLiteralSeq = literal.getFieldConfig().getSeqNum();
        if (literal instanceof WildcardStringLiteral) {
          if (wildcardLiteralSeq > 0)
            log.warn("detected multiple wildcard literals - ignoring");
          wildcardLiteralSeq = literal.getFieldConfig().getSeqNum();
        }
      }
      if (wildcardLiteralSeq != maxLiteralSeq)
        return false;
    }

    if (hasContiguousPartialKeyScanFieldValuesMap == null)
      hasContiguousPartialKeyScanFieldValuesMap = new HashMap<DataGraphMapping, Boolean>();

    if (this.hasContiguousPartialKeyScanFieldValuesMap.get(graph) == null) {
      boolean hasContiguousPartialKeyScanFieldValues = true;

      int size = graph.getUserDefinedRowKeyFields().size();
      int[] scanLiteralCount = initScanLiteralCount(graph);

      // If any field literal 'gap' found, i.e. if no literals found
      // for a field and where the next field DOES have literals
      for (int i = 0; i < size - 1; i++)
        if (scanLiteralCount[i] == 0 && scanLiteralCount[i + 1] > 0)
          hasContiguousPartialKeyScanFieldValues = false;

      this.hasContiguousPartialKeyScanFieldValuesMap.put(graph,
          hasContiguousPartialKeyScanFieldValues);
    }

    return this.hasContiguousPartialKeyScanFieldValuesMap.get(graph).booleanValue();
  }

  /**
   * Returns true if this set of literals can support a partial row key scan for
   * the given graph
   * 
   * @param graph
   *          the graph
   * @return true if this set of literals can support a partial row key scan for
   *         the given graph
   */
  public boolean supportCompleteRowKey(DataGraphMapping graph) {
    if (this.hasWildcardLiterals)
      return false;

    if (!this.hasOnlyEqualityRelationalOperators)
      return false;

    if (hasContiguousKeyFieldValuesMap == null)
      hasContiguousKeyFieldValuesMap = new HashMap<DataGraphMapping, Boolean>();
    if (this.hasContiguousKeyFieldValuesMap.get(graph) == null) {
      boolean hasContiguousFieldValues = true;

      int size = graph.getUserDefinedRowKeyFields().size();
      int[] scanLiteralCount = initScanLiteralCount(graph);

      // If any field literal 'gap' found
      for (int i = 0; i < size; i++)
        if (scanLiteralCount[i] == 0)
          hasContiguousFieldValues = false;

      for (MetaKeyFieldMapping field : graph.getPreDefinedRowKeyFields()) {
        switch (field.getName()) {
        case URI:
        case TYPE:
          break;
        default:
        }
      }

      this.hasContiguousKeyFieldValuesMap.put(graph, hasContiguousFieldValues);
    }

    return this.hasContiguousKeyFieldValuesMap.get(graph).booleanValue();
  }

  private int[] initScanLiteralCount(DataGraphMapping graph) {
    int size = graph.getUserDefinedRowKeyFields().size();
    int[] scanLiteralCount = new int[size];

    for (int i = 0; i < size; i++) {
      DataRowKeyFieldMapping fieldConfig = graph.getUserDefinedRowKeyFields().get(i);
      List<ScanLiteral> list = this.getLiterals(fieldConfig);
      if (list != null)
        scanLiteralCount[i] = list.size();
      else
        scanLiteralCount[i] = 0;
    }

    return scanLiteralCount;

  }

}
