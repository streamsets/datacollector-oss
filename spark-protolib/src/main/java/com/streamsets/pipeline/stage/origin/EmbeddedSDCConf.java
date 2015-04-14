/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

public class EmbeddedSDCConf implements Serializable {
  private EmbeddedPipeline pipeline;
  private List<URL> classPath;

  public EmbeddedSDCConf(EmbeddedPipeline pipeline, List<URL> classPath) {
    this.pipeline = pipeline;
    this.classPath = classPath;
  }

  public EmbeddedPipeline getPipeline() {
    return pipeline;
  }

  public List<URL> getClassPath() {
    return classPath;
  }
}
