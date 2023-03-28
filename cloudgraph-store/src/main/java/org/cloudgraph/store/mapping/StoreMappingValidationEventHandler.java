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
package org.cloudgraph.store.mapping;

import jakarta.xml.bind.ValidationEvent;
import jakarta.xml.bind.ValidationEventLocator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.common.bind.BindingValidationEventHandler;

/**
 * Configuration parsing (JAXB) event handler.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StoreMappingValidationEventHandler implements BindingValidationEventHandler {

  private static Log log = LogFactory.getLog(StoreMappingValidationEventHandler.class);
  private int errorCount;
  private boolean cumulative = true;

  public int getErrorCount() {
    return errorCount;
  }

  public StoreMappingValidationEventHandler() {
  }

  public StoreMappingValidationEventHandler(boolean cumulative) {
    this.cumulative = cumulative;
  }

  public boolean handleEvent(ValidationEvent ve) {
    boolean result = this.cumulative;
    this.errorCount++;
    ValidationEventLocator vel = ve.getLocator();

    String message = "Line:Col:Offset[" + vel.getLineNumber() + ":" + vel.getColumnNumber() + ":"
        + String.valueOf(vel.getOffset()) + "] - " + ve.getMessage();

    switch (ve.getSeverity()) {
    case ValidationEvent.WARNING:
      log.warn(message);
      break;
    case ValidationEvent.ERROR:
    case ValidationEvent.FATAL_ERROR:
      log.fatal(message);
      throw new StoreMappingException(message);
    default:
      log.error(message);
    }
    return result;
  }

  public void reset() {
    this.errorCount = 0;
  }

}
