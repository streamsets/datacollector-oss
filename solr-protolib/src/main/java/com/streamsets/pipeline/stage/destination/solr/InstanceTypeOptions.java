/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum InstanceTypeOptions implements Label {
  SINGLE_NODE("Single Node", SolrTarget.SolrInstanceType.SINGLE_NODE),
  SOLR_CLOUD("SolrCloud",  SolrTarget.SolrInstanceType.SOLR_CLOUD),
  ;


  private final String label;
  private SolrTarget.SolrInstanceType instanceType;

  InstanceTypeOptions(String label, SolrTarget.SolrInstanceType instanceType) {
    this.label = label;
    this.instanceType = instanceType;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public SolrTarget.SolrInstanceType getInstanceType() {
    return instanceType;
  }

}
