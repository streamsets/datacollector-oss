/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class MaskTypeChooseValues extends BaseEnumChooserValues {

  public MaskTypeChooseValues() {
    super(MaskType.class);
  }

}
