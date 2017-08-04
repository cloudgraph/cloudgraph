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
package org.cloudgraph.state;

public class SimpleStateMarshallingContext implements StateMarshalingContext {
  private NonValidatingDataBinding binding;

  @SuppressWarnings("unused")
  private SimpleStateMarshallingContext() {
  }

  public SimpleStateMarshallingContext(NonValidatingDataBinding binding) {
    this.binding = binding;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.StateMarshalingContext#getBinding()
   */
  @Override
  public NonValidatingDataBinding getBinding() {
    return binding;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.StateMarshalingContext#returnDataBinding(org.cloudgraph
   * .state.NonValidatingDataBinding)
   */
  @Override
  public void returnBinding(NonValidatingDataBinding binding) {
  }

}
