/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.Field;

public class LogFieldCreator implements FieldCreator {

  public LogFieldCreator() {
  }

  @Override
  public Field createField(byte[] bytes) {
    return Field.create(new String(bytes));
  }

}
