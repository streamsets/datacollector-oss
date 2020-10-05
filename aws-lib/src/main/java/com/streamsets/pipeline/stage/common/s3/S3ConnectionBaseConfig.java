/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.common.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.origin.s3.Errors;
import com.streamsets.pipeline.stage.origin.s3.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public abstract class S3ConnectionBaseConfig {

  public static final String AWS_CONFIG_PREFIX = "connection.awsConfig.";
  private static final Logger LOG = LoggerFactory.getLogger(S3ConnectionBaseConfig.class);

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    connectionType = AwsS3Connection.TYPE,
    defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
    label = "Connection",
    group = "#0",
    displayPosition = -500
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public AwsS3Connection connection;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Common Prefix",
      description = "",
      displayPosition = 30,
      group = "#0")
  public String commonPrefix;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Delimiter",
      description = "",
      defaultValue = "/",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0")
  public String delimiter;

  @ConfigDef(required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Path Style Address Model",
      defaultValue = "false",
      description = "If checked data is accessed using https://<s3_server>/<bucket>/<path>.  If unchecked, data is " +
          "accessed using Virtual Model style of https://<bucket>.<s3_server>/<path>",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0")
  public boolean usePathAddressModel = false;

  private AmazonS3 s3Client;

  public AmazonS3 getS3Client() {
    return s3Client;
  }

  /* Max Error retries >=0 are set in ClientConfig for S3Client, < 0 will use default (3) */
  public void init(
      Stage.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues,
      int maxErrorRetries
  ) {
    commonPrefix = AWSUtil.normalizePrefix(commonPrefix, delimiter);
    try {
      s3Client = S3ConnectionCreator.createS3Client(
          connection,
          context,
          configPrefix,
          issues,
          maxErrorRetries,
          usePathAddressModel
      );
    } catch (StageException ex) {
      LOG.debug(Errors.S3_SPOOLDIR_20.getMessage(), ex.toString(), ex);
      issues.add(
          context.createConfigIssue(
              Groups.S3.name(),
              configPrefix + S3ConnectionBaseConfig.AWS_CONFIG_PREFIX + "awsAccessKeyId",
              Errors.S3_SPOOLDIR_20,
              ex.toString()
          )
      );
    }
  }

  public void destroy() {
    S3ConnectionCreator.destroyS3Client(s3Client);
  }
}
