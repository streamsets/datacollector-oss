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
package com.streamsets.pipeline.stage.lib.aws;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class TransferManagerConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Thread Pool Size For Parallel Uploads",
      description = "Size of thread pool to use when making requests to Amazon S3",
      displayPosition = 8010,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public int threadPoolSize = 10;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "268435456",
      label = "Multipart Upload Threshold",
      description = "Size threshold in bytes for when to use multipart uploads",
      displayPosition = 8020,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public long multipartUploadThreshold = 16 * 1024 * 1024;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5242880",
      label = "Minimum Upload Part Size",
      description = "Minimum part size in bytes for upload parts",
      displayPosition = 8030,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public long minimumUploadPartSize = 5 * 1024 * 1024;
}
