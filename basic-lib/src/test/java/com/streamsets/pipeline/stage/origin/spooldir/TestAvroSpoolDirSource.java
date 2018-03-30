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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.lib.dirspooler.LocalFileSystem;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import org.junit.Test;

import java.io.File;

public class TestAvroSpoolDirSource {
  private WrappedFile createAvroDataFile() throws Exception {
    File f = AvroSpoolDirSourceTestUtil.createAvroDataFile();
    return new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(f.getAbsolutePath());
  }

  private SpoolDirSource createSource() {
    return new SpoolDirSource(AvroSpoolDirSourceTestUtil.getConf());
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource();
    AvroSpoolDirSourceTestUtil.testProduceFullFile(SpoolDirDSource.class, source, createAvroDataFile());
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    SpoolDirSource source = createSource();
    AvroSpoolDirSourceTestUtil.testProduceLessThanFile(SpoolDirDSource.class, source, createAvroDataFile());
  }
}
