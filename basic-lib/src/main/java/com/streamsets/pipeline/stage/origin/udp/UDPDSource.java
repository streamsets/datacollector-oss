/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.configurablestage.DSource;

import java.util.List;

@StageDef(
  version = "1.0.0",
  label = "UDP Source",
  description = "Listens for UDP messages on a single port",
  icon="udp.png"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class UDPDSource extends DSource {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.LIST,
    label = "Port",
    defaultValue = "[\"9995\"]",
    description = "Port to listen on",
    group = "UDP",
    displayPosition = 10
  )
  public List<String> ports; // string so we can listen on multiple ports in the future

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    defaultValue = "NETFLOW",
    group = "UDP",
    displayPosition = 20
  )
  @ValueChooser(UDPDataFormatChooserValues.class)
  public UDPDataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Max Batch Size (messages)",
    group = "UDP",
    displayPosition = 30,
    min = 0,
    max = Integer.MAX_VALUE
  )
  public int batchSize;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Batch Wait Time (ms)",
    description = "Max time to wait for data before sending a batch",
    displayPosition = 40,
    group = "UDP",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int maxWaitTime;

  @Override
  protected Source createSource() {
    Utils.checkNotNull(dataFormat, "Data format cannot be null");
    Utils.checkState(dataFormat == UDPDataFormat.NETFLOW, "Unknown data format: " + dataFormat);
    return new UDPSource(ports, batchSize, maxWaitTime);
  }
}
