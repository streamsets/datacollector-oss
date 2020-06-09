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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineFragmentConfigBean;
import com.streamsets.datacollector.creation.PipelineFragmentConfigUpgrader;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.validation.Issue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.streamsets.datacollector.configupgrade.TestPipelineConfigurationUpgrader.SERVICE_DEF;
import static com.streamsets.datacollector.configupgrade.TestPipelineConfigurationUpgrader.SOURCE2_V2_DEF;
import static com.streamsets.datacollector.configupgrade.TestPipelineConfigurationUpgrader.getBuildInfo;
import static com.streamsets.datacollector.configupgrade.TestPipelineConfigurationUpgrader.getLibrary;

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
        getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(
        getLibrary(SOURCE2_V2_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf = new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
        .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
        .withStageVersion(SOURCE2_V2_DEF.getVersion())
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
        getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
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
        getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(
        getLibrary(SOURCE2_V2_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf =
        new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
            .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
            .withStageVersion(SOURCE2_V2_DEF.getVersion())
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
        getLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF),
        pipelineFragmentConf,
        issues
    ));
    Assert.assertFalse(issues.isEmpty());
  }

  private PipelineFragmentConfiguration getPipelineFragmentConfiguration() {
    StageConfiguration stageConf =
        new StageConfigurationBuilder("i", TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getName())
            .withLibrary(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getLibrary())
            .withStageVersion(TestPipelineConfigurationUpgrader.SOURCE2_V1_DEF.getVersion())
            .build();

    PipelineInfo info = new PipelineInfo(
        "pipelineId",
        "Title",
        "Description",
        new Date(),
        new Date(),
        "jenkins",
        "jenkins",
        "10",
        UUID.randomUUID(),
        true,
        Collections.emptyMap(),
        "3.17.0",
        "sdcId"
    );

    PipelineFragmentConfiguration conf = new PipelineFragmentConfiguration(
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

    conf.setPipelineInfo(info);
    return conf;
  }

  /**
   * Ensure that upgrading from a future version dies in a clear way.
   */
  @Test
  public void testUpgradeFromTheFuture() throws Exception {
    PipelineFragmentConfiguration pipelineConf = getPipelineFragmentConfiguration();
    pipelineConf.getInfo().setSdcVersion("99.99.99");
    FragmentConfigurationUpgrader up = FragmentConfigurationUpgrader.get();

    List<Issue> issues = new ArrayList<>();

    pipelineConf = up.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), getBuildInfo(), pipelineConf, issues);

    Assert.assertNull(pipelineConf);
    Assert.assertFalse(issues.isEmpty());

    Assert.assertEquals(1, issues.size());
    Assert.assertEquals("VALIDATION_0096", issues.get(0).getErrorCode());
  }

  /**
   * Ensure that we update the sdc version field on upgrade.
   */
  @Test
  public void testUpgradeFromThePast() throws Exception {
    PipelineFragmentConfiguration pipelineConf = getPipelineFragmentConfiguration();
    pipelineConf.getInfo().setSdcVersion("1.0.0");
    FragmentConfigurationUpgrader up = FragmentConfigurationUpgrader.get();

    List<Issue> issues = new ArrayList<>();

    pipelineConf = up.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), getBuildInfo(), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());

    Assert.assertEquals("3.17.0", pipelineConf.getInfo().getSdcVersion());
  }
}
