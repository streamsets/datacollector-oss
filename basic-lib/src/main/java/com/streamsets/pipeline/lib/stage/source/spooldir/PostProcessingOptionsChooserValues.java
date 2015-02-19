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
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;

import java.util.List;

public class PostProcessingOptionsChooserValues extends BaseEnumChooserValues {

  public PostProcessingOptionsChooserValues() {
    super(PostProcessingOptions.class);
  }

}
