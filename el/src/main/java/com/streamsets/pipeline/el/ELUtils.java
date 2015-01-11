/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ELUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ELUtils.class);

  public static ELEvaluator.Variables parseConstants(Map<String,?> constants) throws StageException {
    ELEvaluator.Variables variables = new ELEvaluator.Variables();
    if (constants != null) {
      for (Map.Entry<String, ?> entry : constants.entrySet()) {
        variables.addVariable(entry.getKey(), entry.getValue());
        LOG.debug("Variable: {}='{}'", entry.getKey(), entry.getValue());
      }
    }
    return variables;
  }
}
