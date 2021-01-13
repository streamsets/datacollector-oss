/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.upgrader;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

public class SelectorStageUpgrader implements StageUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(SelectorStageUpgrader.class);

  private StageUpgrader legacyUpgrader;
  private YamlStageUpgrader yamlUpgrader;

  public SelectorStageUpgrader(String stageName, StageUpgrader legacyUpgrader, URL yamlResource) {
    this.legacyUpgrader = legacyUpgrader;
    if (yamlResource != null) {
      this.yamlUpgrader = new YamlStageUpgraderLoader(stageName, yamlResource).get();
    }
  }

  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    boolean hasYamlUpgrades = yamlUpgrader != null && yamlUpgrader.getMinimumConfigVersionHandled()
        != YamlStageUpgrader.NO_CONFIG_VERSIONS_HANDLED
        && yamlUpgrader.getMinimumConfigVersionHandled() < context.getToVersion();

    if (legacyUpgrader != null) {
      int toLegacyVersion = (hasYamlUpgrades) ? yamlUpgrader.getMinimumConfigVersionHandled()
                                              : context.getToVersion();

      if (toLegacyVersion > context.getFromVersion()) {
        LOG.debug(
            "Running legacy upgrader to upgrade '{}' stage from '{}' version to '{}' version",
            context.getStageName(),
            context.getFromVersion(),
            toLegacyVersion
        );
        MutableVersionRangeContextWrapper legacyContext = new MutableVersionRangeContextWrapper(context);
        legacyContext.setToVersion(toLegacyVersion);
        configs = legacyUpgrader.upgrade(configs, legacyContext);
      }
    }

    if (hasYamlUpgrades) {
      int fromYamlVersion = Math.max(context.getFromVersion(), yamlUpgrader.getMinimumConfigVersionHandled());
      MutableVersionRangeContextWrapper yamlContext = new MutableVersionRangeContextWrapper(context);
      yamlContext.setFromVersion(fromYamlVersion);
      LOG.debug(
          "Running YAML upgrader to upgrade '{}' stage from '{}' version to '{}' version",
          yamlContext.getStageInstance(),
          yamlContext.getFromVersion(),
          yamlContext.getToVersion()
      );
      configs = yamlUpgrader.upgrade(configs, yamlContext);
    }

    return configs;
  }

  static class MutableVersionRangeContextWrapper implements Context {

    private Context context;
    private int toVersion;
    private int fromVersion;

    MutableVersionRangeContextWrapper(Context context) {
      this.context = context;
      this.fromVersion = context.getFromVersion();
      this.toVersion = context.getToVersion();
    }

    @Override
    public String getLibrary() {
      return context.getLibrary();
    }

    @Override
    public String getStageName() {
      return context.getStageName();
    }

    @Override
    public String getStageInstance() {
      return context.getStageInstance();
    }

    @Override
    public int getFromVersion() {
      return fromVersion;
    }

    public void setFromVersion(int fromVersion) {
      if (fromVersion > 0) {
        this.fromVersion = fromVersion;
      }
    }

    @Override
    public int getToVersion() {
      return toVersion;
    }

    public void setToVersion(int toVersion) {
      this.toVersion = toVersion;
    }

    @Override
    public void registerService(Class aClass, List<Config> list) {
      context.registerService(aClass, list);
    }
  }

  /**
   * Returns a SelectorStageUpgrader suitable for unit testing that stage's upgrader.  Not for production use (the
   * stageName is populated with a dummy value).
   *
   * @param stageClass the stage class (needs to have {@link StageDef} annotation)
   * @return a {@link SelectorStageUpgrader} for the given stage class
   */
  public static SelectorStageUpgrader createTestInstanceForStageClass(Class<? extends Stage> stageClass) {
    final StageDef stageDef = stageClass.getAnnotation(StageDef.class);
    try {
      return new SelectorStageUpgrader(
          "testStage",
          stageDef.upgrader().newInstance(),
          stageClass.getClassLoader().getResource(stageDef.upgraderDef())
      );
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
