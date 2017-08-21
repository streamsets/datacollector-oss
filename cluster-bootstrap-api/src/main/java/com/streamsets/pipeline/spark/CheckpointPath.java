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
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.Utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class CheckpointPath {

  private final String checkPointPath;

  private CheckpointPath (String checkPointPath) {
    this.checkPointPath = checkPointPath;
  }

  public String getPath() {
    return checkPointPath;
  }

  public static class Builder {

    private final String baseCheckpointDir;
    private String sdcId;
    private String topic;
    private String consumerGroup;
    private String pipelineName;

    Builder(String baseCheckpointDir) {
      this.baseCheckpointDir = baseCheckpointDir;
    }

    public Builder sdcId(String sdcId) {
      this.sdcId = sdcId;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = encode(topic);
      return this;
    }

    public Builder consumerGroup(String consumerGroup) {
      this.consumerGroup = encode(consumerGroup);
      return this;
    }

    public Builder pipelineName(String pipelineName) {
      this.pipelineName = encode(pipelineName);
      return this;
    }

    public CheckpointPath build() {
      return new CheckpointPath(buildString());
    }

    private String buildString() {
      Utils.checkArgument(this.baseCheckpointDir != null && !this.baseCheckpointDir.isEmpty(),
          "Checkpoint base dir: '{}' cannot be null or empty",
          this.baseCheckpointDir
      );
      Utils.checkArgument(this.pipelineName != null && !this.pipelineName.isEmpty(),
          "Pipeline Name: '{}' cannot be null or empty",
          this.pipelineName
      );
      Utils.checkArgument(
          this.sdcId != null && !this.sdcId.isEmpty(),
          "SDC Id: '{}' cannot be null or empty",
          this.sdcId
      );
      Utils.checkArgument(
          this.topic != null && !this.topic.isEmpty(),
          "Topic: '{}' cannot be null or empty",
          this.topic
      );
      Utils.checkArgument(this.consumerGroup != null && !this.consumerGroup.isEmpty(),
          "ConsumerGroup: '{}' cannot be null or empty",
          this.consumerGroup
      );
      StringBuilder sb = new StringBuilder();
      sb.append(this.baseCheckpointDir).
          append("/").
          append(this.sdcId).
          append("/").
          append(topic).
          append("/").
          append(consumerGroup).
          append("/").
          append(pipelineName);
      return sb.toString();
    }
  }

  static String encode(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Could not find UTF-8: " + e, e);
    }
  }
}
