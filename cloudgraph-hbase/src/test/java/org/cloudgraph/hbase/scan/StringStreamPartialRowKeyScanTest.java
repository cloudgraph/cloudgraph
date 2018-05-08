/*
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

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;

import java.io.IOException;
import java.util.Date;

import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QStringNode;

import commonj.sdo.DataGraph;
//import junit.framework.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * String SDO datatype specific partial row-key stream scan operations test.
 * 
 * @author Scott Cinnamond
 * @since 1.1.3
 */

public class StringStreamPartialRowKeyScanTest extends StringScanTest {
  private static Log log = LogFactory.getLog(StringStreamPartialRowKeyScanTest.class);

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testStream() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    QStringNode query = createSelect();
    Observable<DataGraph> stream = this.service.findAsStream(query);
    stream.subscribe(new Observer<DataGraph>() {
      @Override
      public void onSubscribe(Disposable d) {
      }

      @Override
      public void onNext(DataGraph graph) {
        String xml = null;
        try {
          xml = serializeGraph(graph);
        } catch (IOException e) {
        }
        log.info("GRAPH: " + xml);
      }

      @Override
      public void onError(Throwable e) {
        fail();
      }

      @Override
      public void onComplete() {
        log.info("complete");
      }
    });
  }
}
