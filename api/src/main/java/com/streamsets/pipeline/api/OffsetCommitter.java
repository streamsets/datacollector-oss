/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

/**
 * If implemented by a source, the data collector will not keep track of the offsets locally. It will rely
 * on the source functionality for it.
 */
public interface OffsetCommitter {

  public void commit(String offset) throws StageException;

}
