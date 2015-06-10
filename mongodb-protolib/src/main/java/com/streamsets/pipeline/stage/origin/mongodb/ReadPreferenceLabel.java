/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.mongodb;

import com.mongodb.ReadPreference;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum ReadPreferenceLabel implements Label {
  PRIMARY("Primary", ReadPreference.primary()),
  PRIMARY_PREFERRED("Primary Preferred", ReadPreference.primaryPreferred()),
  SECONDARY("Secondary", ReadPreference.secondary()),
  SECONDARY_PREFERRED("Secondary Preferred", ReadPreference.secondaryPreferred()),
  NEAREST("Nearest", ReadPreference.nearest()),
  ;

  private final String label;
  private final ReadPreference readPreference;

  ReadPreferenceLabel(String label, ReadPreference readPreference) {
    this.label = label;
    this.readPreference = readPreference;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public ReadPreference getReadPreference() {
    return readPreference;
  }

}
