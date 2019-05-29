/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.datalake.gen2;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.conf.DataLakeGen2BaseConfig;
import com.streamsets.pipeline.stage.origin.hdfs.HdfsSourceConfigBean;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;

public class DataLakeGen2SourceConfigBean extends HdfsSourceConfigBean {

  @ConfigDefBean
  public DataLakeGen2BaseConfig dataLakeConfig;

  @Override
  public void init(final Stage.Context context, List<Stage.ConfigIssue> issues) {
    dataLakeConfig.init(this, context, issues);
    super.init(context, issues);
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    try {
      return userUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.newInstance(new URI(hdfsUri), hdfsConfiguration);
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
