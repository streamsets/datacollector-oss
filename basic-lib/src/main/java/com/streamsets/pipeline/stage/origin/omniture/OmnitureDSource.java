/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.stage.origin.omniture;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.configurablestage.DSource;
import org.apache.commons.lang3.StringEscapeUtils;

@StageDef(
    version = "1.0.0",
    label = "Omniture",
    description = "Retrieves Omniture reports via the REST API.",
    icon="omniture_icon.png"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class OmnitureDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Omniture REST URL",
      defaultValue = "https://api2.omniture.com/admin/1.4/rest/",
      description = "Specify the Omniture REST endpoint",
      displayPosition = 10,
      group = "OMNITURE"
  )
  public String resourceUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      label = "Omniture Report Description",
      description = "Report description to queue",
      displayPosition = 10,
      mode = ConfigDef.Mode.JSON,
      dependsOn = "httpMode",
      lines = 5,
      triggeredByValue = "POLLING",
      group = "REPORT"
  )
  public String reportDescription;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Request Timeout",
      defaultValue = "3000",
      description = "HTTP request timeout in milliseconds.",
      displayPosition = 20,
      group = "OMNITURE"
  )
  public long requestTimeoutMillis;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      defaultValue = "POLLING",
      displayPosition = 25,
      group = "OMNITURE"
  )
  @ValueChooser(HttpClientModeChooserValues.class)
  public HttpClientMode httpMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Delay between report requests (ms)",
      defaultValue = "5000",
      displayPosition = 26,
      group = "OMNITURE",
      dependsOn = "httpMode",
      triggeredByValue = "POLLING"
  )
  public long pollingInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Batch Size",
      defaultValue = "1",
      description = "Maximum number of response entities to queue (e.g. JSON objects).",
      displayPosition = 40,
      group = "OMNITURE"
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Batch Wait Time (ms)",
      defaultValue = "5000",
      description = "Maximum amount of time to wait to fill a batch before sending it",
      displayPosition = 40,
      group = "OMNITURE"
  )
  public long maxBatchWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      description = "Omniture Username",
      displayPosition = 10,
      group = "OMNITURE"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Shared Secret",
      description = "Omniture Shared Secret",
      displayPosition = 20,
      group = "OMNITURE"
  )
  public String sharedSecret;

  @Override
  protected Source createSource() {
    OmnitureConfig config = new OmnitureConfig();
    config.setPollingInterval(pollingInterval);
    config.setMaxBatchWaitTime(maxBatchWaitTime);
    config.setBatchSize(batchSize);
    config.setHttpMode(httpMode);
    config.setReportDescription(reportDescription);
    config.setRequestTimeoutMillis(requestTimeoutMillis);
    config.setResourceUrl(resourceUrl);
    config.setSharedSecret(sharedSecret);
    config.setUsername(username);

    return new OmnitureSource(config);
  }
}
