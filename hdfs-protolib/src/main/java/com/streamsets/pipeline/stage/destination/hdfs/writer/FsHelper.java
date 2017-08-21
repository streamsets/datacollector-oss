/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

interface FsHelper {
  Path getPath(FileSystem fs, Date recordDate, Record record) throws StageException, IOException;
  void commitOldFiles(FileSystem fs) throws StageException, IOException;
  void handleAlreadyExistingFile(FileSystem fs, Path tempPath) throws IOException, StageException;
  Path renameAndGetPath(FileSystem fs, Path tempPath) throws IOException, StageException;
  OutputStream create(FileSystem fs, Path path) throws IOException;
  StreamCloseEventHandler<?> getStreamCloseEventHandler();
}
