/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

public interface ClassLoaderReleaser {

  public void releaseStageClassLoader(ClassLoader classLoader);

}
