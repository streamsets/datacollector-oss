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
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorStageContext;
import com.streamsets.datacollector.antennadoctor.engine.jexl.JavaNamespace;
import com.streamsets.datacollector.antennadoctor.engine.jexl.SdcJexl;
import com.streamsets.datacollector.antennadoctor.engine.jexl.StageIssueJexl;
import com.streamsets.datacollector.util.Version;
import com.streamsets.pipeline.api.AntennaDoctorMessage;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.lib.el.CollectionEL;
import com.streamsets.pipeline.lib.el.FileEL;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JxltEngine;
import org.apache.commons.jexl3.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main computing engine for Antenna Doctor.
 */
public class AntennaDoctorEngine {
  private static final Logger LOG = LoggerFactory.getLogger(AntennaDoctorEngine.class);

  /**
   * Rules that will be used to classify issues.
   */
  private final List<RuntimeRule> rules;

  /**
   * Main evaluation engine.
   */
  private final JexlEngine engine;

  /**
   * Templating engine for messages.
   */
  private final JxltEngine templateEngine;

  public AntennaDoctorEngine(AntennaDoctorContext context, List<AntennaDoctorRuleBean> rules) {
    ImmutableList.Builder<RuntimeRule> builder = ImmutableList.builder();

    Map<String, Object> namespaces = new HashMap<>();
    // Standard ELs shared with rest of SDC
    namespaces.put("collection", CollectionEL.class);
    namespaces.put("file", FileEL.class);
    // Antenna Doctor special namespace additions
    namespaces.put("java", JavaNamespace.class);

    // Main engine used to evaluate all expressions and templates
    engine = new JexlBuilder()
      .cache(512)
      .strict(true)
      .silent(false)
      .debug(true)
      .namespaces(namespaces)
      .create();
    templateEngine = engine.createJxltEngine();

    // Context for preconditions
    JexlContext jexlContext = new MapContext();
    jexlContext.set("version", new Version(context.getBuildInfo().getVersion()));
    jexlContext.set("sdc", new SdcJexl(context));

    // Evaluate each rule to see if it's applicable to this runtime (version, stage libs, ...)
    for(AntennaDoctorRuleBean ruleBean : rules) {
      LOG.trace("Loading rule {}", ruleBean.getUuid());

      // We're running in SDC and currently only in STAGE 'mode', other modes will be added later
      if(ruleBean.getEntity() == null || !ruleBean.getEntity().isOneOf(AntennaDoctorRuleBean.Entity.STAGE, AntennaDoctorRuleBean.Entity.REST, AntennaDoctorRuleBean.Entity.VALIDATION)) {
        continue;
      }

      // Reset the variables that the rule is keeping
      jexlContext.set("context", new HashMap<>());

      for(String precondition: ruleBean.getPreconditions()) {
        try {
          LOG.trace("Evaluating precondition: {}", precondition);
          if(!evaluateCondition(precondition, jexlContext)) {
            LOG.trace("Precondition {} failed, skipping rule {}", precondition, ruleBean.getUuid());
            continue;
          }
        } catch (Throwable e ) {
          LOG.error("Precondition {} failed, skipping rule {}: {}", precondition, ruleBean.getUuid(), e.getMessage(), e);
          continue;
        }
      }

      // The rule was accepted and should be loaded, so we pre-compute the starting context
      Map<String, Object> startingContext = new HashMap<>();
      jexlContext.set("context", startingContext);
      for(String expr : ruleBean.getStartingContext()) {
        try {
          LOG.trace("Evaluating starting context: {}", expr);
          if(!evaluateCondition(expr, jexlContext)) {
            LOG.trace("Starting context '{}' failed, skipping rule {}", expr, ruleBean.getUuid());
            continue;
          }
        } catch(Throwable e) {
          LOG.error("Starting context '{}' failed, skipping rule {}: {}", expr, ruleBean.getUuid(), e.getMessage(), e);
          continue;
        }
      }

      // All checks passed, so we will accept this rule
      builder.add(new RuntimeRule(ruleBean, startingContext));
    }

    this.rules = builder.build();
    LOG.info("Loaded new Antenna Doctor engine with {} rules", this.rules.size());
  }

  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, Exception exception) {
    JexlContext jexlContext = new MapContext();
    jexlContext.set("issue", new StageIssueJexl(exception));
    jexlContext.set("stageDef", context.getStageDefinition());
    jexlContext.set("stageConf", context.getStageConfiguration());
    return evaluate(context, AntennaDoctorRuleBean.Entity.STAGE, jexlContext);
  }

  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, ErrorCode errorCode, Object... args) {
    JexlContext jexlContext = new MapContext();
    jexlContext.set("issue", new StageIssueJexl(errorCode, args));
    jexlContext.set("stageDef", context.getStageDefinition());
    jexlContext.set("stageConf", context.getStageConfiguration());
    return evaluate(context, AntennaDoctorRuleBean.Entity.STAGE, jexlContext);
  }

  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, String errorMessage) {
    JexlContext jexlContext = new MapContext();
    jexlContext.set("issue", new StageIssueJexl(errorMessage));
    jexlContext.set("stageDef", context.getStageDefinition());
    jexlContext.set("stageConf", context.getStageConfiguration());
    return evaluate(context, AntennaDoctorRuleBean.Entity.STAGE, jexlContext);
  }

  public List<AntennaDoctorMessage> onRest(AntennaDoctorContext context, ErrorCode errorCode, Object... args) {
    JexlContext jexlContext = new MapContext();
    jexlContext.set("issue", new StageIssueJexl(errorCode, args));
    return evaluate(context, AntennaDoctorRuleBean.Entity.REST, jexlContext);
  }

  public List<AntennaDoctorMessage> onRest(AntennaDoctorContext context, Exception exception) {
    JexlContext jexlContext = new MapContext();
    jexlContext.set("issue", new StageIssueJexl(exception));
    return evaluate(context, AntennaDoctorRuleBean.Entity.REST, jexlContext);
  }

  public List<AntennaDoctorMessage> onValidation(AntennaDoctorStageContext context, String groupName, String configName, ErrorCode errorCode, Object... args) {
    JexlContext jexlContext = new MapContext();
    jexlContext.set("issue", new StageIssueJexl(groupName, configName, errorCode, args));
    jexlContext.set("stageDef", context.getStageDefinition());
    jexlContext.set("stageConf", context.getStageConfiguration());
    return evaluate(context, AntennaDoctorRuleBean.Entity.VALIDATION, jexlContext);
  }

  private List<AntennaDoctorMessage> evaluate(
    AntennaDoctorContext context,
    AntennaDoctorRuleBean.Entity entity,
    JexlContext jexlContext
  ) {
    // All our expressions have the sdc object available
    jexlContext.set("sdc", new SdcJexl(context));

    ImmutableList.Builder<AntennaDoctorMessage> builder = ImmutableList.builder();

    // Iterate over rules and try to match them
    for(RuntimeRule rule : this.rules) {
      // Static check to execute only relevant rules
      if(rule.getEntity() != entity) {
        continue;
      }

      // Reset the variables that the rule is keeping
      jexlContext.set("context", new HashMap<>(rule.getStartingContext()));

      // Firstly evaluate conditions
      boolean matched = true;
      for (String condition : rule.getConditions()) {
        LOG.trace("Evaluating rule {} condition {}", rule.getUuid(), condition);
        try {
          if(!evaluateCondition(condition, jexlContext)) {
            matched = false;
            break;
          }
        } catch (JexlException e) {
          matched = false;
          LOG.error("Failed to evaluate rule {} condition {}: {}", rule.getUuid(), condition, e.getMessage(), e);
          break;
        }
      }

      // If all rules succeeded, evaluate message
      if (matched) {
        LOG.trace("Rule {} matched!", rule.getUuid());
        try {
          StringWriter summaryWriter = new StringWriter();
          StringWriter descriptionWriter = new StringWriter();

          LOG.trace("Evaluating summary for rule {}: {}", rule.getUuid(), rule.getSummary());
          templateEngine.createTemplate(rule.getSummary()).evaluate(jexlContext, summaryWriter);

          LOG.trace("Evaluating description for rule {}: {}", rule.getUuid(), rule.getDescription());
          templateEngine.createTemplate(rule.getDescription()).evaluate(jexlContext, descriptionWriter);

          builder.add(new AntennaDoctorMessage(summaryWriter.toString(), descriptionWriter.toString()));
        } catch (JexlException e) {
          LOG.error("Failed to evaluate message for rule {}: {}", rule.getUuid(), e.getMessage(), e);
        }
      } else {
        LOG.trace("Rule {} did not match", rule.getUuid());
      }
    }

    return builder.build();
  }

  private boolean evaluateCondition(String condition, JexlContext context) {
    Object output = engine.createExpression(condition).evaluate(context);

    if(output != null && Boolean.class.isAssignableFrom(output.getClass())) {
      return (boolean)output;
    }

    // We consider any non-boolean value as true (continue)
    return true;
  }
}
