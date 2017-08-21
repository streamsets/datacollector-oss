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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.definition.RawSourceDefinitionExtractor;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

public class TestRawSourceDefinitionExtractor {

  public static class Previewer  implements RawSourcePreviewer {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "X",
        required = true
    )
    public String config;

    @Override
    public InputStream preview(int maxLength) {
      return null;
    }

    @Override
    public String getMimeType() {
      return null;
    }

    @Override
    public void setMimeType(String mimeType) {
    }
  }

  public static class SourceWithoutPreviewer extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @RawSource(rawSourcePreviewer = Previewer.class, mimeType = "M/M")
  public static class SourceWithPreviewer extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @Test
  public void testExtractNoPreviewer() {
    Assert.assertNull(RawSourceDefinitionExtractor.get().extract(SourceWithoutPreviewer.class, "x"));

  }

  @Test
  public void testExtractPreviewer() {
    RawSourceDefinition def = RawSourceDefinitionExtractor.get().extract(SourceWithPreviewer.class, "x");
    Assert.assertEquals(Previewer.class.getName(), def.getRawSourcePreviewerClass());
    Assert.assertEquals("M/M", def.getMimeType());
    Assert.assertEquals(1, def.getConfigDefinitions().size());
    Assert.assertEquals("config", def.getConfigDefinitions().get(0).getName());
  }
}
