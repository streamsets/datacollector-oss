/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

import java.util.List;

public interface Record {

  public List<String> getFieldNames();

  public boolean hasField(String fieldName);

  public Metadata getMetadata(String fieldName);

  public <T> T getValue(String fieldName, Class<T> klass);

  public byte[] getRawValue();

  public List<String> getProcessingPath();

}
