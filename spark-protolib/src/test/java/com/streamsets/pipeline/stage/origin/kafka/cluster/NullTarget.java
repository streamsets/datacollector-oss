/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;

import java.util.Iterator;
//TODO - Only for testing, Remove from here
public class NullTarget extends BaseTarget {

  @Override
  public void write(Batch batch) throws StageException {
  }

}
