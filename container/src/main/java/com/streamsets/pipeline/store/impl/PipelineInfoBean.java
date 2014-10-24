/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.store.impl;

import com.streamsets.pipeline.store.PipelineInfo;

import java.util.Date;

public class PipelineInfoBean implements PipelineInfo {
  private String name;
  private String description;
  private Date created;
  private Date lastModified;
  private String creator;
  private String lastModifier;
  private String lastRev;

  public PipelineInfoBean() {
  }

  public PipelineInfoBean(String name, String description, Date created, Date lastModified, String creator,
      String lastModifier, String lastRev) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.lastModified = lastModified;
    this.creator = creator;
    this.lastModifier = lastModifier;
    this.lastRev = lastRev;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  @Override
  public Date getLastModified() {
    return lastModified;
  }

  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }

  @Override
  public String getCreator() {
    return creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  @Override
  public String getLastModifier() {
    return lastModifier;
  }

  public void setLastModifier(String lastModifier) {
    this.lastModifier = lastModifier;
  }

  @Override
  public String getLastRev() {
    return lastRev;
  }

  public void setLastRev(String lastRev) {
    this.lastRev = lastRev;
  }

}
