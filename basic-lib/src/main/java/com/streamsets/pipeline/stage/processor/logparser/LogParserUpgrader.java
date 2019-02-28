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

package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.log.LogParserService;
import com.streamsets.pipeline.config.OnParseError;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogParserUpgrader implements StageUpgrader {

  private static final String DEFAULT_GROK_PATTERN = "%{COMMONAPACHELOG}";

  @Override
  public List<Config> upgrade(
      List<Config> configs,
      Context context
  ) throws StageException {
    switch (context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs, context);
        if (context.getToVersion() == 2) {
          break;
        }
        // fall through
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }

    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs, Context context) {
    // Take DataFormatParser configs
    List<Config> logParserConfigs = configs.stream()
                                                  .filter(config -> !(
                                                      "fieldPathToParse".equals(config.getName()) ||
                                                          "parsedFieldPath".equals(config.getName()) ||
                                                          "stageOnRecordError".equals(config.getName()) ||
                                                          "stageRequiredFields".equals(config.getName()) ||
                                                          "stageRecordPreconditions".equals(config.getName()) ||
                                                          "compression".equals(config.getName()) ||
                                                          "filePatternInArchive".equals(config.getName())
                                                  ))
                                                  .map(config -> new Config("logParserServiceConfig." + config.getName(),
                                                      config.getValue()
                                                  ))
                                                  .collect(Collectors.toList());

    // Add required configs if not yet added
    if (logParserConfigs.stream().noneMatch(config -> "logParserServiceConfig.logMaxObjectLen".equals(config
        .getName()))) {
      logParserConfigs.add(new Config("logParserServiceConfig.logMaxObjectLen", 1024));
    }

    if (logParserConfigs.stream().noneMatch(config -> "logParserServiceConfig.retainOriginalLine".equals(config
        .getName()))) {
      logParserConfigs.add(new Config("logParserServiceConfig.retainOriginalLine", false));
    }

    if (logParserConfigs.stream().noneMatch(config -> "logParserServiceConfig.onParseError".equals(config
        .getName()))) {
      logParserConfigs.add(new Config("logParserServiceConfig.onParseError", OnParseError.ERROR));
    }

    if (logParserConfigs.stream().noneMatch(config -> "logParserServiceConfig.maxStackTraceLines".equals(config
        .getName()))) {
      logParserConfigs.add(new Config("logParserServiceConfig.maxStackTraceLines", 50));
    }

    if (logParserConfigs.stream().noneMatch(config -> "logParserServiceConfig.charset".equals(config
        .getName()))) {
      logParserConfigs.add(new Config("logParserServiceConfig.charset", "UTF-8"));
    }

    // search grok pattern string
    Optional<Config> grokPatternConfig = logParserConfigs.stream().filter(config -> {
      return config.getName().contains("grokPattern") && !config.getName().contains("grokPatternDefinition");
    }).findFirst();

    // remove grok pattern string
    logParserConfigs.removeIf(config -> {
      return config.getName().contains("grokPattern") && !config.getName().contains("grokPatternDefinition");
    });

    // add new grok pattern list config to configs list
    if (grokPatternConfig.isPresent()) {
      logParserConfigs.add(new Config(
          "logParserServiceConfig.grokPatternList",
          Arrays.asList((String) (grokPatternConfig.get().getValue()))
      ));
    } else {
      logParserConfigs.add(new Config("logParserServiceConfig.grokPatternList", Arrays.asList(DEFAULT_GROK_PATTERN)));
    }

    // Remove old DataFormatParser configs from LogParser configs
    configs.removeIf(config -> {
      boolean shouldRemove;
      switch (config.getName()) {
        case "fieldPathToParse":
        case "parsedFieldPath":
        case "stageOnRecordError":
        case "stageRequiredFields":
        case "stageRecordPreconditions":
          shouldRemove = false;
          break;
        default:
          shouldRemove = true;
          break;
      }
      return shouldRemove;
    });

    // Add the LogParserService with its corresponding configs
    context.registerService(LogParserService.class, logParserConfigs);
  }

}
