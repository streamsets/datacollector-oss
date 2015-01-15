/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;

import java.util.List;

public class PostProcessingOptionsChooserValues implements ChooserValues {
  private static final List<String> VALUES;

  static {
    VALUES = ImmutableList.copyOf(Lists.transform(ImmutableList.copyOf(DirectorySpooler.FilePostProcessing.values()),
                                                  new Function<DirectorySpooler.FilePostProcessing, String>() {
                                                    @Override
                                                    public String apply(DirectorySpooler.FilePostProcessing input) {
                                                      return input.toString();
                                                    }
                                                  }));
  }

  @Override
  public List<String> getValues() {
    return VALUES;
  }

  @Override
  public List<String> getLabels() {
    return VALUES;
  }
}
