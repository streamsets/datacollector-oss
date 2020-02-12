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

package com.streamsets.pipeline.stage.origin.datalake.gen1;

import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.WrappedFileSystem;
import com.streamsets.pipeline.stage.origin.datalake.AzureDirectorySpooler;
import com.streamsets.pipeline.stage.origin.datalake.AzureHdfsFileSystem;
import com.streamsets.pipeline.stage.origin.hdfs.HdfsSource;

public class DataLakeSource extends HdfsSource {

  public DataLakeSourceConfig conf;

  public DataLakeSource(SpoolDirConfigBean spoolDirConf, DataLakeSourceConfig dataLakeSourceConfigBean) {
    super(spoolDirConf, dataLakeSourceConfigBean);
    this.conf = dataLakeSourceConfigBean;
  }

  @Override
  public WrappedFileSystem getFs() {
    return new AzureHdfsFileSystem(super.conf.filePattern, super.conf.pathMatcherMode,
        super.conf.processSubdirectories, hdfsSourceConfigBean.getFileSystem());
  }

  @Override
  protected DirectorySpooler.Builder getDirectorySpoolerBuilder() {
    return new AzureDirectorySpooler.Builder();
  }
}
