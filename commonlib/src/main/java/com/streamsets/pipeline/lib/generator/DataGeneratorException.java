/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;

public class DataGeneratorException extends StageException {

  public DataGeneratorException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}
