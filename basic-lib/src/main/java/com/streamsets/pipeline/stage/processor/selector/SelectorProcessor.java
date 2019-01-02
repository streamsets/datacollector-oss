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
package com.streamsets.pipeline.stage.processor.selector;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.ELUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SelectorProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SelectorProcessor.class);

  private final List<Map<String, String>> lanePredicates;

  public SelectorProcessor(List<Map<String, String>> lanePredicates) {
    this.lanePredicates = lanePredicates;
  }

  private String[][] predicateLanes;
  private ELEval predicateLanesEval;
  private ELVars variables;
  private String defaultLane;

  private ELEval createPredicateLanesEval(ELContext elContext) {
    return elContext.createELEval("lanePredicates");
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (lanePredicates == null || lanePredicates.size() == 0) {
      issues.add(getContext().createConfigIssue(Groups.CONDITIONS.name(), "lanePredicates", Errors.SELECTOR_00));
    } else {
      if (getContext().getOutputLanes().size() != lanePredicates.size()) {
        issues.add(getContext().createConfigIssue(Groups.CONDITIONS.name(), "lanePredicates", Errors.SELECTOR_01,
                                                  lanePredicates.size(),
                                                  getContext().getOutputLanes().size()));
      } else {
        predicateLanes = parsePredicateLanes(lanePredicates, issues);
        if (!predicateLanes[predicateLanes.length - 1][0].equals("default")) {
          issues.add(getContext().createConfigIssue(Groups.CONDITIONS.name(), "lanePredicates", Errors.SELECTOR_07));
        } else {
          variables = ELUtils.parseConstants(null, getContext(), Groups.CONDITIONS.name(), "constants",
                                             Errors.SELECTOR_04, issues);
          predicateLanesEval = createPredicateLanesEval(getContext());
          RecordEL.setRecordInContext(variables, getContext().createRecord("forValidation"));
          for (int i = 0; i < predicateLanes.length - 1; i++) {
            String[] predicateLane = predicateLanes[i];
            if (!predicateLane[0].startsWith("${") || !predicateLane[0].endsWith("}")) {
              issues.add(getContext().createConfigIssue(Groups.CONDITIONS.name(), "lanePredicates", Errors.SELECTOR_08,
                                                        predicateLane[0]));
            } else {
              ELUtils.validateExpression(predicateLane[0], getContext(),
                                         Groups.CONDITIONS.name(), "lanePredicates", Errors.SELECTOR_03, issues);
            }
          }
        }
        defaultLane = predicateLanes[predicateLanes.length - 1][1];
      }
    }
    return issues;
  }

  private String[][] parsePredicateLanes(List<Map<String, String>> predicateLanesList, List<ConfigIssue> issues) {
    String[][] predicateLanes = new String[predicateLanesList.size()][];
    for (int i = 0; i < predicateLanesList.size(); i++) {
      Map<String, String> predicateLaneMap = predicateLanesList.get(i);
      final String outputLaneSuffix = predicateLaneMap.get("outputLane");
      String outputLane = outputLaneSuffix;
      Object predicate = predicateLaneMap.get("predicate");

      // pipeline fragments alter output lane names, therefore outputLane
      // should be appended to composite lane name, rather than exact match.
      // normal (not pipeline fragment) case is unaffected
      Optional<String> maybeOutputLane = getContext().getOutputLanes().stream().filter(lane -> lane.endsWith(outputLaneSuffix)).findAny();
      if (!maybeOutputLane.isPresent()) {
        issues.add(getContext().createConfigIssue(Groups.CONDITIONS.name(), "lanePredicates", Errors.SELECTOR_02,
                                                  outputLaneSuffix, predicate));
      } else {
        outputLane = maybeOutputLane.get();
      }
      predicateLanes[i] = new String[2];
      predicateLanes[i][0] = (String) predicate;
      predicateLanes[i][1] = outputLane;
      LOG.debug("Condition:'{}' to stream:'{}'", predicate, outputLane);
    }
    return predicateLanes;
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    boolean matchedAtLeastOnePredicate = false;
    RecordEL.setRecordInContext(variables, record);
    for (int i = 0; i < predicateLanes.length - 1; i ++) {
      String[] pl = predicateLanes[i];
      try {
        if (predicateLanesEval.eval(variables, pl[0], Boolean.class)) {
          LOG.trace("Record '{}' satisfies condition '{}', going to '{}' output stream",
                    record.getHeader().getSourceId(), pl[0], pl[1]);
          batchMaker.addRecord(record, pl[1]);
          matchedAtLeastOnePredicate = true;
        }
      } catch (ELEvalException ex) {
        throw new OnRecordErrorException(Errors.SELECTOR_09, record.getHeader().getSourceId(), pl[0], ex.toString(),
                                         ex);
      }
    }
    if (!matchedAtLeastOnePredicate) {
      LOG.trace("Record '{}' does not satisfy any condition, going to default output stream",
                record.getHeader().getSourceId());
      batchMaker.addRecord(record, defaultLane);
    }
  }

}
