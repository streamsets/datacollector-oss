/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum AwsRegion implements Label {
  US_EAST_2("US East (Ohio) us-east-2"),
  US_EAST_1("US East (N. Virginia) us-east-1"),
  US_WEST_1("US West (N. California) us-west-1"),
  US_WEST_2("US West (Oregon) us-west-2"),
  US_GOV_WEST_1("AWS GovCloud (US)"),
  AP_NORTHEAST_1("Aisa Pacific (Tokyo) ap-northeast-1"),
  AP_NORTHEAST_2("Asia Pacific (Seoul) ap-northeast-2"),
  AP_NORTHEAST_3("Asia Pacific (Osaka-Local) ap-northeast-3"),
  AP_SOUTH_1("Asia Pacific (Mumbai) ap-south-1"),
  AP_SOUTHEAST_1("Asia Pacific (Singapore) ap-southeast-1"),
  AP_SOUTHEAST_2("Asia Pacific (Sydney) ap-southeast-2"),
  CA_CENTRAL_1("Canada (Central) ca-central-1"),
  CN_NORTH_1("China (Beijing) cn-north-1"),
  CN_NORTHWEST_1("China (Ningxia) cn-northwest-1"),
  EU_CENTRAL_1("EU (Frankfurt) eu-central-1"),
  EU_WEST_1("EU (Ireland) eu-west-1"),
  EU_WEST_2("EU (London) eu-west-2"),
  EU_WEST_3("EU (Paris) eu-west-3"),
  SA_EAST_1("South America (São Paulo) sa-east-1"),
  OTHER("Other - specify")
  ;

  private final String label;

  AwsRegion(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getId() {
    return name().toLowerCase().replace('_', '-');
  }

}
