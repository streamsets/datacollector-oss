/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.container.LocalizableMessage;
import com.streamsets.pipeline.container.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class PipelineDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineDefinition.class);

  private static final String PIPELINE_RESOURCE_BUNDLE = "PipelineDefinition-bundle";
  private final static String CONFIG_DELIVERY_GUARANTEE_LABEL_KEY = "config.deliveryGuarantee.label";
  private final static String CONFIG_DELIVERY_GUARANTEE_DESCRIPTION_KEY = "config.deliveryGuarantee.description";
  private final static String CONFIG_STOP_ON_ERROR_LABEL_KEY = "config.stopOnError.label";
  private final static String CONFIG_STOP_ON_ERROR_DESCRIPTION_KEY = "config.stopOnError.description";
  private final static String DELIVERY_GUARANTEE_AT_LEAST_ONCE_KEY = "config.deliveryGuarantee.AT_LEAST_ONCE";
  private final static String DELIVERY_GUARANTEE_AT_LEAST_ONCE_VALUE = "At Least Once";
  private final static String DELIVERY_GUARANTEE_AT_MOST_ONCE_KEY = "config.deliveryGuarantee.AT_MOST_ONCE";
  private final static String DELIVERY_GUARANTEE_AT_MOST_ONCE_VALUE = "At Most Once";
  private final static String DELIVERY_GUARANTEE_LABEL_VALUE = "Delivery Guarantee";
  private final static String DELIVERY_GUARANTEE_DESCRIPTION_VALUE = "This is the option for the delivery guarantee";
  private static final String CONFIG_STOP_ON_ERROR_LABEL_VALUE = "Stop On Error";
  private static final String CONFIG_STOP_ON_ERROR_DESCRIPTION_VALUE = "This is the option for Stop on Error";

  /*The config definitions of the pipeline*/
  private List<ConfigDefinition> configDefinitions;

  public PipelineDefinition() {
    configDefinitions = new ArrayList<>(2);
    configDefinitions.add(createDeliveryGuaranteeOption());
    configDefinitions.add(createStopOnErrorOption());
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

  private ConfigDefinition createStopOnErrorOption() {
    String seLabel = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        CONFIG_STOP_ON_ERROR_LABEL_KEY, CONFIG_STOP_ON_ERROR_LABEL_VALUE, null).getLocalized();
    String seDescription = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        CONFIG_STOP_ON_ERROR_DESCRIPTION_KEY, CONFIG_STOP_ON_ERROR_DESCRIPTION_VALUE, null).getLocalized();

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

  private ConfigDefinition createDeliveryGuaranteeOption() {

    List<String> gdLabels = new ArrayList<String>(2);
    gdLabels.add(new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        DELIVERY_GUARANTEE_AT_LEAST_ONCE_KEY, DELIVERY_GUARANTEE_AT_LEAST_ONCE_VALUE, null).getLocalized());
    gdLabels.add(new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        DELIVERY_GUARANTEE_AT_MOST_ONCE_KEY, DELIVERY_GUARANTEE_AT_MOST_ONCE_VALUE, null).getLocalized());

    List<String> gdValues = new ArrayList<String>(2);
    gdValues.add(DeliveryGuarantee.AT_LEAST_ONCE.name());
    gdValues.add(DeliveryGuarantee.AT_MOST_ONCE.name());

    ModelDefinition gdModelDefinition = new ModelDefinition(ModelType.VALUE_CHOOSER,
      ChooserMode.PROVIDED, "",  gdValues, gdLabels);

    //Localize label and description for "delivery guarantee" config option
    String dgLabel = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        CONFIG_DELIVERY_GUARANTEE_LABEL_KEY, DELIVERY_GUARANTEE_LABEL_VALUE, null).getLocalized();
    String dgDescription = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        CONFIG_DELIVERY_GUARANTEE_DESCRIPTION_KEY, DELIVERY_GUARANTEE_DESCRIPTION_VALUE, null).getLocalized();

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
