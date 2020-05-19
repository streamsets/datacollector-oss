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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.stage.lib.aws.AWSCredentialMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFirehoseTargetUpgrader {

  @Test
  public void testUpgradeV4toV5() {
    FirehoseTargetUpgrader  firehoseTargetUpgrader = new FirehoseTargetUpgrader();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("kinesisConfig.awsConfig.awsAccessKeyId", null));
    configs.add(new Config("kinesisConfig.awsConfig.awsSecretAccessKey", null));
    firehoseTargetUpgrader.upgrade("a", "b", "c", 4, 5, configs);
    assertEquals("kinesisConfig.awsConfig.credentialMode", configs.get(2).getName());
    assertEquals(AWSCredentialMode.WITH_IAM_ROLES, configs.get(2).getValue());

    configs = new ArrayList<>();
    configs.add(new Config("kinesisConfig.awsConfig.awsAccessKeyId", "key"));
    configs.add(new Config("kinesisConfig.awsConfig.awsSecretAccessKey", "secret"));
    firehoseTargetUpgrader.upgrade("a", "b", "c", 4, 5, configs);
    assertEquals("kinesisConfig.awsConfig.credentialMode", configs.get(2).getName());
    assertEquals(AWSCredentialMode.WITH_CREDENTIALS, configs.get(2).getValue());
  }

}
