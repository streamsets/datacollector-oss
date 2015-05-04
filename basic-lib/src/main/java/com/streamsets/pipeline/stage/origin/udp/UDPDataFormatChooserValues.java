/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

/**
 * This is just a dummy class until the udp source supports
 * other data formats.
 */
public class UDPDataFormatChooserValues extends BaseEnumChooserValues<UDPDataFormat> {
  public UDPDataFormatChooserValues() {
    super(UDPDataFormat.class);
  }
}
