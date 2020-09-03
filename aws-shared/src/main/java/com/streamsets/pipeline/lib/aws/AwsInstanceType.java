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
package com.streamsets.pipeline.lib.aws;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum AwsInstanceType implements Label {
  M4_LARGE("m4.large"),
  M4_XLARGE("m4.xlarge"),
  M4_2XLARGE("m4.2xlarge"),
  M4_4XLARGE("m4.4xlarge"),
  M4_10XLARGE("m4.10xlarge"),
  M4_16XLARGE("m4.16xlarge"),

  C4_LARGE("c4.large"),
  C4_XLARGE("c4.xlarge"),
  C4_2XLARGE("c4.2xlarge"),
  C4_4XLARGE("c4.4xlarge"),
  C4_8XLARGE("c4.8xlarge"),

  R3_XLARGE("r3.xlarge"),
  R3_2XLARGE("r3.2xlarge"),
  R3_4XLARGE("r3.4xlarge"),
  R3_8XLARGE("r3.8xlarge"),
  R4_XLARGE("r4.xlarge"),
  R4_2XLARGE("r4.2xlarge"),
  R4_4XLARGE("r4.4xlarge"),
  R4_8XLARGE("r4.8xlarge"),
  R4_16XLARGE("r4.16xlarge"),

  I3_XLARGE("i3.xlarge"),
  I3_2XLARGE("i3.2xlarge"),
  I3_4XLARGE("i3.4xlarge"),
  I3_8XLARGE("i3.8xlarge"),
  I3_16XLARGE("i3.16xlarge"),
  D2_XLARGE("d2.xlarge"),
  D2_2XLARGE("d2.2xlarge"),
  D2_4XLARGE("d2.4xlarge"),
  D2_8XLARGE("d2.8xlarge"),

  P2_XLARGE("p2.xlarge"),
  P2_8XLARGE("p2.8xlarge"),
  P2_16XLARGE("p2.16xlarge"),
  P3_2XLARGE("p3.2xlarge"),
  P3_8XLARGE("p3.8xlarge"),
  P3_16XLARGE("p3.16xlarge"),

  OTHER("Custom")
  ;

  private final String label;

  AwsInstanceType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getId() {
    return name().toLowerCase().replace('_', '.');
  }

}
