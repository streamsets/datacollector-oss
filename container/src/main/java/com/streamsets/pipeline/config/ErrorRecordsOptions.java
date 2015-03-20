/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public enum ErrorRecordsOptions implements Label {
  TRASH("Discard", "streamsets-datacollector-basic-lib:com_streamsets_pipeline_lib_stage_destination_NullTarget:1.0.0"),
  DISK("Save to Disk", "streamsets-datacollector-basic-lib:com_streamsets_pipeline_lib_stage_destination_DiskErrorRecordsTarget:1.0.0")
  ;

  public final String label;
  public final String stage;

  ErrorRecordsOptions(String label, String stage) {
    this.label = label;
    this.stage = stage;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
