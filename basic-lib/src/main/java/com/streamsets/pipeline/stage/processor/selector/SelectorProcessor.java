/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.selector;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Stream Selector",
    description = "Passes records to streams based on conditions",
    icon="laneSelector.png",
    outputStreams = StageDef.VariableOutputStreams.class,
    outputStreamsDrivenByConfig = "lanePredicates")
@ConfigGroups(Groups.class)
public class SelectorProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SelectorProcessor.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Condition",
      description = "Records that match the condition pass to the stream",
      displayPosition = 10,
      group = "CONDITIONS"
  )
  @LanePredicateMapping
  public List<Map<String, String>> lanePredicates;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MAP,
      label = "Constants",
      description = "Can be used in any expression in the processor",
      displayPosition = 20,
      group = "CONDITIONS"
  )
  public Map<String, ?> constants;

  private String[][] predicateLanes;
  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;
  private String defaultLane;

  @Override
  protected List<ConfigIssue> validateConfigs()  throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    if (lanePredicates == null || lanePredicates.size() == 0) {
      issues.add(getContext().createConfigIssue(Errors.SELECTOR_00));
    } else {
      if (getContext().getOutputLanes().size() != lanePredicates.size()) {
        issues.add(getContext().createConfigIssue(Errors.SELECTOR_01, lanePredicates.size(),
                                                  getContext().getOutputLanes().size()));
      } else {
        predicateLanes = parsePredicateLanes(lanePredicates, issues);
        if (!predicateLanes[predicateLanes.length - 1][0].equals("default")) {
          issues.add(getContext().createConfigIssue(Errors.SELECTOR_07));
        } else {
          variables = parseConstants(constants);
          elEvaluator = new ELEvaluator();
          ELRecordSupport.registerRecordFunctions(elEvaluator);
          ELStringSupport.registerStringFunctions(elEvaluator);
          ELRecordSupport.setRecordInContext(variables, getContext().createRecord("forValidation"));
          for (int i = 0; i < predicateLanes.length - 1; i++) {
            String[] predicateLane = predicateLanes[i];
            if (!predicateLane[0].startsWith("${") || !predicateLane[0].endsWith("}")) {
              issues.add(getContext().createConfigIssue(Errors.SELECTOR_08, predicateLane[0]));
            } else {
              try {
                elEvaluator.eval(variables, predicateLane[0], Boolean.class);
              } catch (ELException ex) {
                issues.add(getContext().createConfigIssue(Errors.SELECTOR_03, predicateLane[0], ex.getMessage(), ex));
              }
            }
          }
        }
      }
    }
    return issues;
  }

  private String[][] parsePredicateLanes(List<Map<String, String>> predicateLanesList, List<ConfigIssue> issues)
      throws StageException {
    String[][] predicateLanes = new String[predicateLanesList.size()][];
    int count = 0;
    for (int i = 0; i < predicateLanesList.size(); i++) {
      Map<String, String> predicateLaneMap = predicateLanesList.get(i);
      String outputLane = predicateLaneMap.get("outputLane");
      Object predicate = predicateLaneMap.get("predicate");
      if (!getContext().getOutputLanes().contains(outputLane)) {
        issues.add(getContext().createConfigIssue(Errors.SELECTOR_02, outputLane, predicate));
      }
      predicateLanes[count] = new String[2];
      predicateLanes[count][0] = (String) predicate;
      predicateLanes[count][1] = outputLane;
      LOG.debug("Condition:'{}' Stream:'{}'", predicate, outputLane);
      count++;
    }
    return predicateLanes;
  }

  @SuppressWarnings("unchecked")
  private ELEvaluator.Variables parseConstants(Map<String,?> constants) throws StageException {
    ELEvaluator.Variables variables = new ELEvaluator.Variables();
    if (constants != null) {
      for (Map.Entry<String, ?> entry : constants.entrySet()) {
        variables.addVariable(entry.getKey(), entry.getValue());
        LOG.debug("Variable: {}='{}'", entry.getKey(), entry.getValue());
      }
    }
    return variables;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    defaultLane = predicateLanes[predicateLanes.length - 1][1];
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    boolean matchedAtLeastOnePredicate = false;
    ELRecordSupport.setRecordInContext(variables, record);
    for (int i = 0; i < predicateLanes.length - 1; i ++) {
      String[] pl = predicateLanes[i];
      try {
        if (elEvaluator.eval(variables, pl[0], Boolean.class)) {
          LOG.trace("Record '{}' satisfies condition '{}', going to '{}' output stream",
                    record.getHeader().getSourceId(), pl[0], pl[1]);
          batchMaker.addRecord(record, pl[1]);
          matchedAtLeastOnePredicate = true;
        }
      } catch (ELException ex) {
        throw new OnRecordErrorException(Errors.SELECTOR_04, record.getHeader().getSourceId());
      }
    }
    if (!matchedAtLeastOnePredicate) {
      LOG.trace("Record '{}' does not satisfy any condition, going to default output stream",
                record.getHeader().getSourceId());
      batchMaker.addRecord(record, defaultLane);
    }
  }

}
