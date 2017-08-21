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
package com.streamsets.datacollector.bundles.content;

import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;
import com.streamsets.datacollector.bundles.Constants;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@BundleContentGeneratorDef(
  name = "Pipelines",
  description = "Pipeline metadata (definition, history files, ...).",
  version = 1,
  enabledByDefault = true
)
public class PipelineContentGenerator implements BundleContentGenerator {


  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    Pattern redactionPattern = Pattern.compile(
      context.getConfiguration().get(Constants.PIPELINE_REDACT_REGEXP, Constants.DEFAULT_PIPELINE_REDACT_REGEXP)
    );

    PipelineStoreTask store = context.getPipelineStore();
    PipelineStateStore stateStore = context.getPipelineStateStore();

    try {
      for(PipelineInfo pipelineInfo : store.getPipelines()) {
        String name = pipelineInfo.getPipelineId();
        String rev = pipelineInfo.getLastRev();

        // Info
        writer.writeJson(name + "/info.json", BeanHelper.wrapPipelineInfo(pipelineInfo));

        // Pipeline (format is for "exported" pipeline, so that it can be easily imported)
        PipelineConfiguration pipelineConfig = store.load(name, rev);
        RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
        redactPipeline(pipelineConfig, redactionPattern);
        PipelineEnvelopeJson pipelineEnvelope = new PipelineEnvelopeJson();
        pipelineEnvelope.setPipelineConfig(BeanHelper.wrapPipelineConfiguration(pipelineConfig));
        pipelineEnvelope.setPipelineRules(BeanHelper.wrapRuleDefinitions(ruleDefinitions));

        writer.writeJson(name + "/pipeline.json", pipelineEnvelope);

        // History data
        writer.writeJson(name + "/history.json", BeanHelper.wrapPipelineStatesNewAPI(stateStore.getHistory(name, rev, true), false));

        // Offset data
        Map<String, String> offsets = OffsetFileUtil.getOffsets(context.getRuntimeInfo(), name, rev);
        writer.writeJson(name + "/offset.json", BeanHelper.wrapSourceOffset(new SourceOffset(SourceOffset.CURRENT_VERSION, offsets)));
      }
    } catch (PipelineException e) {
      throw new IOException("Can't load pipelines", e);
    }
  }

  private void redactPipeline(PipelineConfiguration pipelineConfig, Pattern redactionPattern) {
    redactConfigs(pipelineConfig.getConfiguration(), redactionPattern);

    for(StageConfiguration stage : pipelineConfig.getStages()) {
      List<Config> configs = stage.getConfiguration();
      redactConfigs(configs, redactionPattern);
      stage.setConfig(configs);
    }
  }

  private void redactConfigs(List<Config> configuration, Pattern redactionPattern) {
    List<Config> toAdd = new ArrayList<>();
    List<Config> toRemove = new ArrayList<>();

    for(Config config: configuration) {
      if(redactionPattern.matcher(config.getName()).matches()) {
        Object newValue = config.getValue();
        if(newValue instanceof String) {
          String value = (String) newValue;
          if(value.startsWith("${") && value.endsWith("}")) {
            // Seems that this config is an EL, so we will not redact it (it should not be sensitive)
          } else {
            // All other cases will be redacted
            newValue = "REDACTED";
          }
        }

        toRemove.add(config);
        toAdd.add(new Config(
          config.getName(),
          newValue
        ));
      }
    }

    configuration.removeAll(toRemove);
    configuration.addAll(toAdd);
  }

}
