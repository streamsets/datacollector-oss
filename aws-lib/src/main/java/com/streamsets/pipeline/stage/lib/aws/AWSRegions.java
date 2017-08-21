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

import com.streamsets.pipeline.api.Label;

public enum AWSRegions implements Label {
  // Copied from com.amazonaws.regions.Region except for OTHER
  GovCloud("us-gov-west-1"),
  US_EAST_1("us-east-1"),
  US_EAST_2("us-east-2"),
  US_WEST_1("us-west-1"),
  US_WEST_2("us-west-2"),
  EU_WEST_1("eu-west-1"),
  EU_CENTRAL_1("eu-central-1"),
  AP_SOUTHEAST_1("ap-southeast-1"),
  AP_SOUTHEAST_2("ap-southeast-2"),
  AP_NORTHEAST_1("ap-northeast-1"),
  AP_NORTHEAST_2("ap-northeast-2"),
  SA_EAST_1("sa-east-1"),
  CN_NORTH_1("cn-north-1"),
  OTHER("other - specify endpoint")
  ;

  private final String label;

  AWSRegions(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
