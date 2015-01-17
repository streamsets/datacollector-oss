/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;

public interface FieldCreator {

  public Field createField(byte[] bytes) throws StageException;

}
