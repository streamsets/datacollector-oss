/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.hdfs;

import com.streamsets.pipeline.api.Stage;

import com.streamsets.pipeline.lib.hdfs.common.HdfsBaseConfigBean;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;

public class HdfsSourceConfigBean extends HdfsBaseConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSourceConfigBean.class);
  private static final int ONE = 1;

  @Override
  protected String getConfigBeanPrefix() {
    return "hdfsSourceConfigBean.";
  }

  public void init(final Stage.Context context, List<Stage.ConfigIssue> issues) {
    validateHadoopFS(context, issues);
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    try {
      return userUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(new URI(hdfsUri), hdfsConfiguration);
        }
      });
    } catch (RuntimeException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof Exception) {
        throw (Exception)cause;
      }
      throw ex;
    }
  }
}