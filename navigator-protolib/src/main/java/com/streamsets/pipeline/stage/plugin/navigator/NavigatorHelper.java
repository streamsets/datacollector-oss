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

package com.streamsets.pipeline.stage.plugin.navigator;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NavigatorHelper {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorHelper.class);
  private static final int NAVIGATOR_PROPERTY_KEY_LENGTH = 40;
  private static final int NAVIGATOR_PROPERTY_VALUE_LENGTH = 500;

  private NavigatorHelper() {
    throw new IllegalStateException("do not instantiate NavigatorHelper");
  }

  public static String makePipelineIdentity(String pipelineId, long pipelineStartTime) {

    return DigestUtils.md5Hex(pipelineId + Long.toString(pipelineStartTime) + "Pipeline");
  }
  public static String makePipelineInstanceIdentity(String pipelineId, long pipelineStartTime) {

    return DigestUtils.md5Hex(pipelineId + Long.toString(pipelineStartTime) + "Instance");
  }

  public static String makeDatasetIdentity(String pipelineId, String entityName, long pipelineStartTime) {
    return DigestUtils.md5Hex(pipelineId + Long.toString(pipelineStartTime) + entityName + "Dataset");
  }

  public static String nameChopper(String name) {
    // 5.10 and above limit the name length
    if (name.length() > NAVIGATOR_PROPERTY_KEY_LENGTH) {
      name = name.substring(0, NAVIGATOR_PROPERTY_KEY_LENGTH - 1 - 3);
      name += "...";
    }
    return name;
  }

  public static String parameterChopper(String name) {
    // 5.10 and above limit the name length
    if (name.length() > NAVIGATOR_PROPERTY_VALUE_LENGTH) {
      name = name.substring(0, NAVIGATOR_PROPERTY_VALUE_LENGTH - 1) ;
    }
    return name;
  }

}
