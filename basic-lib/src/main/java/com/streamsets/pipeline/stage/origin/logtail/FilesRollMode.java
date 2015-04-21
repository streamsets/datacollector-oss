/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.io.LogRollMode;
import com.streamsets.pipeline.lib.io.PeriodicFilesRollMode;
import com.streamsets.pipeline.lib.io.RollMode;

@GenerateResourceBundle
public enum FilesRollMode implements Label {
  REVERSE_COUNTER("Active File with Reverse Counter Files", LogRollMode.REVERSE_COUNTER, null),
  DATE_YYYY_MM("Active File with .yyyy-MM Files", LogRollMode.DATE_YYYY_MM, null),
  DATE_YYYY_MM_DD("Active File with .yyyy-MM-dd Files", LogRollMode.DATE_YYYY_MM_DD, null),
  DATE_YYYY_MM_DD_HH("Active File with .yyyy-MM-dd-HH Files", LogRollMode.DATE_YYYY_MM_DD_HH, null),
  DATE_YYYY_MM_DD_HH_MM("Active File with .yyyy-MM-dd-HH-mm Files", LogRollMode.DATE_YYYY_MM_DD_HH_MM, null),
  DATE_YYYY_WW("Active File with .yyyy-ww Files", LogRollMode.DATE_YYYY_WW, null),
  ALPHABETICAL("Active File with Alphabetical Files", LogRollMode.ALPHABETICAL, null),
  PERIODIC("Periodic Files Only", null, PeriodicFilesRollMode.class),

  ;

  private final String label;
  private final RollMode mode;
  private final Class<? extends RollMode> klass;

  FilesRollMode(String label, RollMode mode, Class<? extends RollMode> klass) {
    this.label = label;
    this.mode = mode;
    this.klass = klass;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public RollMode createRollMode(String filePattern) {
    if (mode != null) {
      return mode;
    }
    if (klass != null) {
      try {
        RollMode mode = klass.newInstance();
        mode.setPattern(filePattern);
        return mode;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    throw new RuntimeException("mode and klass are both NULL");
  }

}
