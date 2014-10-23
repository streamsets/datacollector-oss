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
package com.streamsets.pipeline.store;

import com.streamsets.pipeline.config.RuntimePipelineConfiguration;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public interface PipelineStore {

  public void init() throws IOException;

  public void destroy();

  public void create(String name) throws IOException;

  public void delete(String name) throws IOException;

  public List<String> getNames() throws IOException;

  public void save(String name, String user, String tag, String description, RuntimePipelineConfiguration pipeline)
      throws IOException;

  public static class RevisionInfo {
    private Date date;
    private String user;
    private String rev;
    private String tag;
    private String description;

    public RevisionInfo(Date date, String user, String rev, String tag, String description) {
      this.date = date;
      this.user = user;
      this.rev = rev;
      this.tag = tag;
      this.description = description;
    }

    public Date getDate() {
      return date;
    }

    public String getUser() {
      return user;
    }

    public String getRev() {
      return rev;
    }

    public String getTag() {
      return tag;
    }
    public String getDescription() {
      return description;
    }

  }

  public List<RevisionInfo> getRevisions(String name) throws IOException;

  public RuntimePipelineConfiguration get(String name, String tag) throws IOException;

}
