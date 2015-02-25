/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

public interface Target extends Stage<Target.Context> {

  public interface Context extends Stage.Context {

    public Record createRecord(String recordSourceId);

  }

  public void write(Batch batch) throws StageException;

}
