/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.sdk.testharness.internal;

import com.streamsets.pipeline.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public abstract class StageBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(StageBuilder.class);

  protected Map<String, Object> configMap = null;
  protected Stage.Info info = null;
  protected Stage.Context context = null;
  protected String instanceName;
  protected Stage stage;
  /*Source offset*/
  protected String sourceOffset = Constants.DEFAULT_SOURCE_OFFSET;
  /*Max records in a batch*/
  protected int maxBatchSize = Constants.DEFAULT_MAX_BATCH_SIZE;

  public StageBuilder() {
    configMap = new HashMap<String, Object>();
  }

  /**
   * Configures the stage instance with the supplied configuration
   * options.
   *
   * @throws StageException
   */
  protected void configureStage() throws StageException {
    //Set configuration options on the instance of source
    for(Map.Entry<String, Object> e : configMap.entrySet()) {
      Field field;
      try {
        field = this.stage.getClass().getField(e.getKey());
        field.set(this.stage, e.getValue());
      } catch (NoSuchFieldException e1) {
        e1.printStackTrace();
      } catch (IllegalAccessException e1) {
        e1.printStackTrace();
      }
    }
  }

  /**
   * Validates the configuration that applied to all stages.
   * @return
   */
  protected boolean validateStage() {
    boolean valid = true;
    /*By this point all the required options must be set*/

    if(instanceName == null || instanceName.isEmpty()) {
      LOG.info("Generating the default instanceName : " + Constants.DEFAULT_INSTANCE_NAME);
      instanceName = Constants.DEFAULT_INSTANCE_NAME;
    }

    if(maxBatchSize == 0) {
      LOG.info("Setting the 'maxBatchSize' to a default value of 10.");
      maxBatchSize = Constants.DEFAULT_MAX_BATCH_SIZE;
    } else if (maxBatchSize < 0) {
      LOG.error("The 'maxBatchSize' must be greater than zero.");
      valid = false;
    }

    if(stage == null) {
      LOG.error("Processor is not set.");
      valid = false;
    }
    
    if(configMap == null || configMap.isEmpty()) {
      LOG.warn("No configuration properties set for this instance of the Source. Ignore this message if no configuration is expected.");
    } else {
      valid = validateConfiguration(configMap, stage);
    }
    return valid;
  }

  /**
   * Validates the supplied configuration values against the ones in the definition
   * @param configMap
   * @param stage
   * @return
   */
  private boolean validateConfiguration(
    Map<String, Object> configMap, Stage stage) {
    boolean valid = true;
    //for each entry in the map check if the key is a valid config option
    ConfigDef configDef = null;
    FieldSelector fieldSelector = null;
    FieldModifier fieldModifier = null;
    for(Field f : stage.getClass().getDeclaredFields()) {
      configDef = f.getAnnotation(ConfigDef.class);
      fieldModifier = f.getAnnotation(FieldModifier.class);
      fieldSelector = f.getAnnotation(FieldSelector.class);
      if(configDef != null) {
        if(!configMap.containsKey(f.getName())) {
          if(configDef.required()) {
            LOG.error("Configuration for a required field {} is missing.", f.getName());
            valid = false;
          } else {
            //populate the field with default value
            String defaultVal = configDef.defaultValue();
            try {
              f.set(stage, getValueFromType(configDef.type(), defaultVal));
            } catch (IllegalAccessException e) {
              e.printStackTrace();

            }
          }
        } else {
          valid = validateType(configDef, fieldModifier, fieldSelector, f, configMap.get(f.getName()));
        }
      }
    }
    //The configuration type must match
    return valid;
  }

  private Object getValueFromType(ConfigDef.Type type, String value) {
    switch(type) {
      case INTEGER:
        return Integer.parseInt(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case MODEL:
        //TODO
      case STRING:
        return value;
      default:
        return null;
    }
  }

  /**
   * Validates that the type of the supplied configuration object
   * matches the type defined in the configuration.
   *
   * @param configDef
   * @param fieldModifier
   * @param fieldSelector
   * @param f
   * @param value
   * @return
   */
  private boolean validateType(ConfigDef configDef,
                               FieldModifier fieldModifier,
                               FieldSelector fieldSelector,
                               Field f,
                               Object value) {
    switch(configDef.type()) {
      case INTEGER:
        if (!(value instanceof Integer)) {
          LOG.error("The configuration value for field '{}' does not match the expected type '{}'",
                    f.getName(), configDef.type().name());
          return false;
        }
        break;
      case BOOLEAN:
        if (!(value instanceof Boolean)) {
          LOG.error("The configuration value for field '{}' does not match the expected type '{}'",
                    f.getName(), configDef.type().name());
          return false;
        }
        break;
      case MODEL:
        if (fieldSelector != null && !(value instanceof Boolean)) {
          LOG.error("The field '{}' is marked as FieldSelector. A boolean value is expected.",
                    f.getName());
          return false;
        }
        if (fieldModifier != null && !(value instanceof String)) {
          LOG.error("The field '{}' is marked as FieldSelector. A String value is expected.",
                    f.getName());
          return false;
        }
        break;
      case STRING:
        if (!(value instanceof String)) {
          LOG.error("The configuration value for field '{}' does not match the expected type '{}'",
                    f.getName(), configDef.type().name());
          return false;
        }
        break;
    }
    return true;
  }

}
