/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

/**
 * If implemented by a source, the data collector will do not keep track locally of offsets, it will relay
 * on the source functionality for such.
 */
public interface OffsetCommitter {

  public void commit(String offset) throws StageException;

}
