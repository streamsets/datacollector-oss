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
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.container.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PipelineDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineDefinition.class);

  private static final String PIPELINE_RESOURCE_BUNDLE = "PipelineDefinition-bundle";
  private final static String CONFIG_DELIVERY_GUARENTEE_LABEL = "config.deliveryGuarantee.label";
  private final static String CONFIG_DELIVERY_GUARENTEE_DESCRIPTION = "config.deliveryGuarantee.description";
  private final static String CONFIG_STOP_ON_ERROR_LABEL = "config.stopOnError.label";
  private final static String CONFIG_STOP_ON_ERROR_DESCRIPTION = "config.stopOnError.description";
  private final static String DELIVERY_GUARENTEE_AT_LEAST_ONCE = "config.deliveryGuarantee.AT_LEAST_ONCE";
  private final static String DELIVERY_GUARENTEE_AT_MOST_ONCE = "config.deliveryGuarantee.AT_MOST_ONCE";

  /*The config definitions of the pipeline*/
  private List<ConfigDefinition> configDefinitions;

  public PipelineDefinition(Locale locale) {
    ResourceBundle rb = getResourceBundle(locale);
    configDefinitions = new ArrayList<ConfigDefinition>(2);
    configDefinitions.add(createDeliveryGuaranteeOption(rb, locale));
    configDefinitions.add(createStopOnErrorOption(rb, locale));
  }

  /*Need this API for Jackson to serialize*/
  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineDefinition[configDefinitions='{}']", configDefinitions);
  }

  /**************************************************************/
  /********************** Private methods ***********************/
  /**************************************************************/

  private ResourceBundle getResourceBundle(Locale locale) {
    ResourceBundle rb = null;
    if (locale != null) {
      try {
        rb = ResourceBundle.getBundle(PIPELINE_RESOURCE_BUNDLE, locale, getClass().getClassLoader());
      } catch (MissingResourceException ex) {
        LOG.warn("Could not find resource bundle '{}'.", PIPELINE_RESOURCE_BUNDLE);
      }
    }
    return rb;
  }

  private ConfigDefinition createStopOnErrorOption(ResourceBundle rb, Locale locale) {
    String seLabel = rb != null ? (rb.containsKey(CONFIG_STOP_ON_ERROR_LABEL) ?
      rb.getString(CONFIG_STOP_ON_ERROR_LABEL) : "Stop On Error")
      : "Stop On Error";
    String seDescription = rb != null ? (rb.containsKey(CONFIG_STOP_ON_ERROR_DESCRIPTION) ?
      rb.getString(CONFIG_STOP_ON_ERROR_DESCRIPTION) : "This is the option for Stop on Error")
      : "This is the option for Stop on Error";

    //create configuration for guaranteed delivery option
    ConfigDefinition seConfigDef = new ConfigDefinition(
      "stopPipelineOnError",
      ConfigDef.Type.BOOLEAN,
      seLabel,
      seDescription,
      "true",
      true,
      "",
      "",
      null);

    return seConfigDef;
  }

  private ConfigDefinition createDeliveryGuaranteeOption(ResourceBundle rb, Locale locale) {

    List<String> gdLabels = new ArrayList<String>(2);
    gdLabels.add(
      rb != null ?
        (rb.containsKey(DELIVERY_GUARENTEE_AT_LEAST_ONCE) ?
          rb.getString(DELIVERY_GUARENTEE_AT_LEAST_ONCE) :
          "At Least Once")
        : "At Least Once");

    gdLabels.add(
      rb != null ?
        (rb.containsKey(DELIVERY_GUARENTEE_AT_MOST_ONCE) ?
          rb.getString(DELIVERY_GUARENTEE_AT_MOST_ONCE) :
          "At Most Once")
        : "At Most Once");

    List<String> gdValues = new ArrayList<String>(2);
    gdValues.add(DeliveryGuarantee.AT_LEAST_ONCE.name());
    gdValues.add(DeliveryGuarantee.AT_MOST_ONCE.name());

    ModelDefinition gdModelDefinition = new ModelDefinition(ModelType.DROPDOWN,
      FieldModifierType.PROVIDED, "",  gdValues, gdLabels);

    //Localize label and description for "delivery guarantee" config option
    String dgLabel = rb != null ? (rb.containsKey(CONFIG_DELIVERY_GUARENTEE_LABEL) ?
      rb.getString(CONFIG_DELIVERY_GUARENTEE_LABEL) : "Delivery Guarantee")
      : "Delivery Guarantee";
    String dgDescription = rb != null ? (rb.containsKey(CONFIG_DELIVERY_GUARENTEE_DESCRIPTION) ?
      rb.getString(CONFIG_DELIVERY_GUARENTEE_DESCRIPTION) : "This is the option for the delivery guarantee")
      : "This is the option for the delivery guarantee";

    ConfigDefinition dgConfigDef = new ConfigDefinition(
      "deliveryGuarantee",
      ConfigDef.Type.MODEL,
      dgLabel,
      dgDescription,
      DeliveryGuarantee.AT_LEAST_ONCE.name(),
      true,
      "",
      "",
      gdModelDefinition);

    return dgConfigDef;
  }
}
