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
package com.streamsets.datacollector.antennadoctor.engine;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRuleBean;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorContext;
import com.streamsets.datacollector.antennadoctor.engine.el.AntennaDoctorELDefinitionExtractor;
import com.streamsets.datacollector.antennadoctor.engine.el.SdcEL;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.util.Version;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Main computing engine for Antenna Doctor.
 */
public class AntennaDoctorEngine {
  private static final Logger LOG = LoggerFactory.getLogger(AntennaDoctorEngine.class);

  /**
   * Rules that will be used to classify issues.
   */
  private final List<RuntimeRule> rules;

  public AntennaDoctorEngine(AntennaDoctorContext context,List<AntennaDoctorRuleBean> rules) {
    ImmutableList.Builder<RuntimeRule> builder = ImmutableList.builder();
    ELEval preconditionEval = new ELEvaluator(null, AntennaDoctorELDefinitionExtractor.get(), SdcEL.class);

    for(AntennaDoctorRuleBean ruleBean : rules) {
      LOG.trace("Loading rule {}", ruleBean.getUuid());

      // Validate min SDC version
      if(ruleBean.getMinSdcVersion() != null) {
        Version minSdcVersion = new Version(ruleBean.getMinSdcVersion());
        Version sdcVersion = new Version(context.getBuildInfo().getVersion());

        if(!sdcVersion.isGreaterOrEqualTo(minSdcVersion)) {
          LOG.trace("Min SDC version check ({} <= {}) failed, skipping rule {}", minSdcVersion.toString(), sdcVersion.toString(), ruleBean.getUuid());
          continue;
        }
      }

      // And similar check to max SDC version (albeit this check is open interval)
      if(ruleBean.getMaxSdcVersion() != null) {
        Version maxSdcVersion = new Version(ruleBean.getMaxSdcVersion());
        Version sdcVersion = new Version(context.getBuildInfo().getVersion());

        if(!sdcVersion.isLessThan(maxSdcVersion)) {
          LOG.trace("Max SDC version check ({} > {}) failed, skipping rule {}", maxSdcVersion.toString(), sdcVersion.toString(), ruleBean.getUuid());
          continue;
        }
      }

      // Evaluate dynamic preconditions
      ELVars vars = preconditionEval.createVariables();
      SdcEL.setVars(vars, context);
      for(String precondition: ruleBean.getPreconditions()) {
        try {
          LOG.trace("Evaluating precondition: {}", precondition);
          if (!preconditionEval.eval(vars, "${" + precondition + "}", Boolean.class)) {
            LOG.trace("Precondition {} failed, skipping rule {}", precondition, ruleBean.getUuid());
            continue;
          }
        } catch (Throwable e ) {
          LOG.error("Precondition {} failed, skipping rule {}: {}", precondition, ruleBean.getUuid(), e.toString(), e);
          continue;
        }
      }

      // All checks passed, so we will accept this rule
      builder.add(new RuntimeRule(ruleBean));
    }

    this.rules = builder.build();
    LOG.info("Loaded new Antenna Doctor engine with {} rules", this.rules.size());
  }
}
