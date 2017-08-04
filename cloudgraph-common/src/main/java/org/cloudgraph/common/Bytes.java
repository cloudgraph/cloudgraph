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
package org.cloudgraph.common;

public class Bytes {
  public static byte[] concat(byte[]... fields) {
    int i = 0;
    for (byte[] field : fields)
      i += field.length;
    byte[] result = new byte[i];
    i = 0;
    for (byte[] field : fields) {
      System.arraycopy(field, 0, result, i, field.length);
      i += field.length;
    }
    return result;
  }

}
