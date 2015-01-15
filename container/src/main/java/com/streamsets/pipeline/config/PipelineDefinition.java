/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;


public class PipelineDefinition {
  private static final String PIPELINE_RESOURCE_BUNDLE = "PipelineDefinition-bundle";

  private final static String DELIVERY_GUARANTEE_LABEL_KEY = "config.deliveryGuarantee.label";
  private final static String DELIVERY_GUARANTEE_LABEL_VALUE = "Delivery Guarantee";

  private final static String DELIVERY_GUARANTEE_DESCRIPTION_KEY = "config.deliveryGuarantee.description";
  private final static String DELIVERY_GUARANTEE_DESCRIPTION_DEFAULT = "This is the option for the delivery guarantee";

  private final static String STOP_ON_ERROR_LABEL_KEY = "config.stopOnError.label";
  private static final String STOP_ON_ERROR_LABEL_VALUE = "Stop On Error";

  private final static String STOP_ON_ERROR_DESCRIPTION_KEY = "config.stopOnError.description";
  private static final String STOP_ON_ERROR_DESCRIPTION_DEFAULT = "This is the option for Stop on Error";

  private final static String DELIVERY_GUARANTEE_AT_LEAST_ONCE_KEY = "config.deliveryGuarantee.AT_LEAST_ONCE";
  private final static String DELIVERY_GUARANTEE_AT_LEAST_ONCE_DEFAULT = "At Least Once";
  private final static String DELIVERY_GUARANTEE_AT_MOST_ONCE_KEY = "config.deliveryGuarantee.AT_MOST_ONCE";
  private final static String DELIVERY_GUARANTEE_AT_MOST_ONCE_DEFAULT = "At Most Once";





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
        STOP_ON_ERROR_LABEL_KEY, STOP_ON_ERROR_LABEL_VALUE, null).getLocalized();
    String seDescription = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        STOP_ON_ERROR_DESCRIPTION_KEY, STOP_ON_ERROR_DESCRIPTION_DEFAULT, null).getLocalized();

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
      null,
      "",
      new String[] {},
      0);

    return seConfigDef;
  }

  private ConfigDefinition createDeliveryGuaranteeOption() {

    List<String> gdLabels = new ArrayList<>(2);
    gdLabels.add(new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        DELIVERY_GUARANTEE_AT_LEAST_ONCE_KEY, DELIVERY_GUARANTEE_AT_LEAST_ONCE_DEFAULT, null).getLocalized());
    gdLabels.add(new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        DELIVERY_GUARANTEE_AT_MOST_ONCE_KEY, DELIVERY_GUARANTEE_AT_MOST_ONCE_DEFAULT, null).getLocalized());

    List<String> gdValues = new ArrayList<>(2);
    gdValues.add(DeliveryGuarantee.AT_LEAST_ONCE.name());
    gdValues.add(DeliveryGuarantee.AT_MOST_ONCE.name());

    ModelDefinition gdModelDefinition = new ModelDefinition(ModelType.VALUE_CHOOSER,
      ChooserMode.PROVIDED, "",  gdValues, gdLabels, null);

    //Localize label and description for "delivery guarantee" config option
    String dgLabel = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        DELIVERY_GUARANTEE_LABEL_KEY, DELIVERY_GUARANTEE_LABEL_VALUE, null).getLocalized();
    String dgDescription = new LocalizableMessage(getClass().getClassLoader(), PIPELINE_RESOURCE_BUNDLE,
        DELIVERY_GUARANTEE_DESCRIPTION_KEY, DELIVERY_GUARANTEE_DESCRIPTION_DEFAULT, null).getLocalized();

    ConfigDefinition dgConfigDef = new ConfigDefinition(
      "deliveryGuarantee",
      ConfigDef.Type.MODEL,
      dgLabel,
      dgDescription,
      DeliveryGuarantee.AT_LEAST_ONCE.name(),
      true,
      "",
      "",
      gdModelDefinition,
      "",
      new String[] {},
      0);

    return dgConfigDef;
  }
}
