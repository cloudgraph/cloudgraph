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
package org.cloudgraph.store.lang;

/**
 * A query language filter assembler
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public interface FilterAssembler {

  public abstract String getFilter();

  public abstract Object[] getParams();

  public abstract String getVariableDeclarations();

  public abstract boolean hasVariableDeclarations();

  public abstract String getImportDeclarations();

  public abstract boolean hasImportDeclarations();

  public abstract String getParameterDeclarations();

  public abstract boolean hasParameterDeclarations();
}