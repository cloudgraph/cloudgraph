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
package org.cloudgraph.store.key;

/**
 * Qualifier or code-set used to describe a graph/row.
 * <p>
 * </p>
 * The name is referenced directly in source code, and the underlying code is
 * used as the actual physical column qualifier. The allows the source to remain
 * more readable while allowing more terse physical qualifiers or column keys.
 * 
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public interface MetaKey {
  public String asString();

  public String code();

  public byte[] codeAsBytes();

  public String name();

  public String description();
}
