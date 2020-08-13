/*
 * Copyright 2020 StreamSets Inc.
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

public enum AWSCredentialMode implements Label {
  WITH_CREDENTIALS("AWS Keys"),
  // IAM Role is not really the right term: https://medium.com/devops-dudes/the-difference-between-an-aws-role-and-an-instance-profile-ae81abd700d
  WITH_IAM_ROLES("Instance Profile"),
  WITH_ANONYMOUS_CREDENTIALS("None"),
  ;

  private final String label;

  AWSCredentialMode(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
