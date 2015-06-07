/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.RawSourceDefinition;
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
