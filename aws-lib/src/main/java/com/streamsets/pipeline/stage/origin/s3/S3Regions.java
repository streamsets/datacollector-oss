/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.Label;

public enum S3Regions implements Label {

  GovCloud("AWS GovCloud (US) - us-gov-west-1"),
  US_EAST_1("US Standard (N. Virginia) - us-east-1"),
  US_WEST_1("US West (N. California) - us-west-1"),
  US_WEST_2("US West (Oregon) - us-west-2"),
  EU_WEST_1("EU (Ireland) - eu-west-1"),
  EU_CENTRAL_1("EU (Frankfurt) - eu-central-1"),
  AP_SOUTHEAST_1("Asia Pacific (Singapore) - ap-southeast-1"),
  AP_SOUTHEAST_2("Asia Pacific (Sydney) - ap-southeast-2"),
  AP_NORTHEAST_1("Asia Pacific (Tokyo) - ap-northeast-1"),
  SA_EAST_1("South America (Sao Paulo) - sa-east-1"),
  CN_NORTH_1("China (Beijing) - cn-north-1");
  ;

  private final String label;

  S3Regions(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
