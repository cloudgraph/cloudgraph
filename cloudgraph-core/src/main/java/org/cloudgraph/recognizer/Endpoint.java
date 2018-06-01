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
package org.cloudgraph.recognizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.Function;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.Property;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * An property qualified by its path from the root of a graph, its type and its
 * unique identifier.
 * 
 * @author Scott Cinnamond
 * @since 1.0.4
 */
public interface Endpoint {

  public PlasmaProperty getProperty();

  public boolean hasQueryProperty();

  public Property getQueryProperty();

  public boolean hasFunctions();

  public Function getSingleFunction();

  public boolean hasPath();

  public String getPath();

  public int getLevel();

}
