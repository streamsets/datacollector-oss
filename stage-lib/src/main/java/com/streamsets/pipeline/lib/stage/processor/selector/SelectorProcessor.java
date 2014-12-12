/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.selector;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.util.Map;
import java.util.Set;

@GenerateResourceBundle
@StageDef(version = "1.0.0", label = "Lane Selector",
    description = "Lane Selector based on record predicates")
public class SelectorProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SelectorProcessor.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Lane/Record-predicate mapping",
      description = "Associates output lanes with predicates records must match in order to go to the lane")
  @LanePredicateMapping
  public Map<String, String> lanePredicates;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Not Matching Predicate Action",
      description = "Action to take with records not matching any predicate",
      defaultValue = "DROP")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = OnNoPredicateMatchChooserValues.class)
  public OnNoPredicateMatch onNoPredicateMatch;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MAP,
      label = "Constants for predicates",
      description = "Defines constant values available to all predicates")
  public Map<String, String> constants;

  private String[][] predicateLanes;
  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;

  @Override
  protected void init() throws StageException {
    super.init();
    if (lanePredicates == null || lanePredicates.size() == 0) {
      throw new StageException(StageLibError.LIB_0010);
    }
    if (getContext().getOutputLanes().size() != lanePredicates.size()) {
      throw new StageException(StageLibError.LIB_0011, getContext().getOutputLanes(), lanePredicates.keySet());
    }
    predicateLanes = new String[lanePredicates.size()][];
    int count = 0;
    for (Map.Entry<String, String> entry : lanePredicates.entrySet()) {
      if (!getContext().getOutputLanes().contains(entry.getKey())) {
        throw new StageException(StageLibError.LIB_0012, entry.getKey(), entry.getValue());
      }
      predicateLanes[count] = new String[2];
      predicateLanes[count][0] = "${" + entry.getValue() + "}";
      predicateLanes[count][1] = entry.getKey();
      LOG.debug("Predicate:'{}' Lane:'{}'", predicateLanes[count][0], predicateLanes[count][1]);
      count++;
    }
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    variables = new ELEvaluator.Variables();
    if (constants != null) {
      for (Map.Entry<String, String> entry : constants.entrySet()) {
        variables.addVariable(entry.getKey(), entry.getValue());
        LOG.debug("Variable: {}='{}'", entry.getKey(), entry.getValue());
      }
    }
    validateELs();
    LOG.debug("All predicates validated");
  }

  private void validateELs() throws StageException {

    Record record = new Record(){
      @Override
      public Header getHeader() {
        return null;
      }

      @Override
      public Field get() {
        return null;
      }

      @Override
      public Field set(Field field) {
        return null;
      }

      @Override
      public Field get(String fieldPath) {
        return null;
      }

      @Override
      public Field delete(String fieldPath) {
        return null;
      }

      @Override
      public boolean has(String fieldPath) {
        return false;
      }

      @Override
      public Set<String> getFieldPaths() {
        return null;
      }
    };

    variables.addVariable("default", false);
    ELRecordSupport.setRecordInContext(variables, record);
    for (String[] predicateLane : predicateLanes) {
      try {
        elEvaluator.eval(variables, predicateLane[0], Boolean.class);
      } catch (ELException ex) {
        throw new StageException(StageLibError.LIB_0013, predicateLane[0], ex.getMessage(), ex);
      }
    }
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    boolean matchedAtLeastOnePredicate = false;
    ELRecordSupport.setRecordInContext(variables, record);
    for (String[] pl : predicateLanes) {
      variables.addVariable("default", !matchedAtLeastOnePredicate);
      try {
        if (elEvaluator.eval(variables, pl[0], Boolean.class)) {
          LOG.trace("Record '{}' satisfies predicate '{}', going to lane '{}'", record.getHeader().getSourceId(),
                    pl[0], pl[1]);
          batchMaker.addRecord(record, pl[1]);
          matchedAtLeastOnePredicate = true;
        } else{
          LOG.trace("Record '{}' does not satisfy predicate '{}', skipping lane '{}'", record.getHeader().getSourceId(),
                    pl[0], pl[1]);
        }
      } catch (ELException ex) {
        getContext().toError(record, StageLibError.LIB_0014, pl[0], ex.getMessage(), ex);
      }
    }
    if (!matchedAtLeastOnePredicate) {
      switch (onNoPredicateMatch) {
        case DROP_RECORD:
          LOG.trace("Record '{}' does not satisfy any predicate, dropping it", record.getHeader().getSourceId());
          break;
        case RECORD_TO_ERROR:
          LOG.trace("Record '{}' does not satisfy any predicate, sending it to error",
                    record.getHeader().getSourceId());
          getContext().toError(record, StageLibError.LIB_0015);
          break;
        case FAIL_PIPELINE:
          LOG.error(StageLibError.LIB_0016.getMessage(), record.getHeader().getSourceId());
          throw new StageException(StageLibError.LIB_0016, record.getHeader().getSourceId());
      }
    }
  }

}
