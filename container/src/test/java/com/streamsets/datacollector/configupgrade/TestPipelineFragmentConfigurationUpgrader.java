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
package com.streamsets.datacollector.configupgrade;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineFragmentConfigBean;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.validation.Issue;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestPipelineFragmentConfigurationUpgrader {

  @Test
  public void testNeedsUpgrade() {
    FragmentConfigurationUpgrader up = FragmentConfigurationUpgrader.get();

    StageConfiguration stageConf =
        new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
            .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
            .withStageVersion(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getVersion())
            .build();

    PipelineFragmentConfiguration pipelineFragmentConf = new PipelineFragmentConfiguration(
        UUID.randomUUID(),
        PipelineFragmentConfigBean.VERSION,
        2,
        "random_title",
        "random_id",
        "random_fragment_instance_id",
        "weird description",
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList(),
        stageConf
    );

    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(
        TestPipelineConfigurationUpgrader.getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(
        TestPipelineConfigurationUpgrader.getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V2_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf = new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
        .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
        .withStageVersion(TestPipelineConfigurationUpgrader.SOURCE2_V2_DEF.getVersion())
        .build();
    pipelineFragmentConf = new PipelineFragmentConfiguration(
        UUID.randomUUID(),
        PipelineFragmentConfigBean.VERSION,
        2,
        "random_title",
        "random_id",
        "random_fragment_instance_id",
        "weird description",
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList(),
        stageConf
    );
    Assert.assertFalse(up.needsUpgrade(
        TestPipelineConfigurationUpgrader.getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testNeedsUpgradePipelineFragmentStage() {
    FragmentConfigurationUpgrader up = FragmentConfigurationUpgrader.get();

    StageConfiguration stageConf =
        new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
            .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
            .withStageVersion(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getVersion())
            .build();

    PipelineFragmentConfiguration pipelineFragmentConf = new PipelineFragmentConfiguration(
        UUID.randomUUID(),
        PipelineFragmentConfigBean.VERSION,
        2,
        "random_title",
        "random_id",
        "random_fragment_instance_id",
        "weird description",
        Collections.emptyList(),
        ImmutableList.of(stageConf),
        Collections.emptyMap(),
        Collections.emptyList(),
        null
    );
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(
        TestPipelineConfigurationUpgrader.getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(
        TestPipelineConfigurationUpgrader.getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V2_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf =
        new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
            .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
            .withStageVersion(TestPipelineConfigurationUpgrader.SOURCE2_V2_DEF.getVersion())
            .build();
    pipelineFragmentConf = new PipelineFragmentConfiguration(
        UUID.randomUUID(),
        PipelineFragmentConfigBean.VERSION,
        2,
        "random_title",
        "random_id",
        "random_fragment_instance_id",
        "weird description",
        Collections.emptyList(),
        ImmutableList.of(stageConf),
        Collections.emptyMap(),
        Collections.emptyList(),
        null
    );
    Assert.assertFalse(up.needsUpgrade(
        TestPipelineConfigurationUpgrader.getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertFalse(issues.isEmpty());
  }

}
