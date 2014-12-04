/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.Iterator;
import java.util.Set;

public interface Batch {

  public String getSourceOffset();

  public Iterator<Record> getRecords();

}
