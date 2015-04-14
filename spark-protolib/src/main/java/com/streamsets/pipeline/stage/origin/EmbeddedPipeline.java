/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import com.streamsets.pipeline.restapi.bean.PipelineConfigurationJson;

import java.io.Serializable;

public class EmbeddedPipeline  implements Serializable {
  private String name;
  private String user;
  private String tag;
  private String description;
  private PipelineConfigurationJson pipelineConfigurationJson;

  public EmbeddedPipeline(String name, String user, String tag, String description,
                          PipelineConfigurationJson pipelineConfigurationJson) {
    this.name = name;
    this.user = user;
    this.tag = tag;
    this.description = description;
    this.pipelineConfigurationJson = pipelineConfigurationJson;
  }

  public String getName() {
    return name;
  }

  public String getUser() {
    return user;
  }

  public String getTag() {
    return tag;
  }

  public String getDescription() {
    return description;
  }

  public PipelineConfigurationJson getDefinitionJson() {
    return pipelineConfigurationJson;
  }
}
