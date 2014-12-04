/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v2;

import java.util.List;

public interface Record {

  public byte[] getRawRecord();

  public List<String> getRecordPipelinePath();

  public SimpleMap<String, String> getEnvelope();

  public Iterable<String> getFieldNames();

  public boolean hasField(String name);

  public Field getField(String name);

  public void setField(String name, Field field);

  public void removeField(String name);

}
