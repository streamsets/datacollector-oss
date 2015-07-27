/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public enum DeliveryGuarantee implements Label {
  AT_LEAST_ONCE("At Least Once"),
  AT_MOST_ONCE("At Most Once")
  ;

  private final String label;

  DeliveryGuarantee(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
