/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.ChooserMode;


@RawSource(rawSourcePreviewer = ClipperSourcePreviewer.class, mimeType = "text/plain")
@StageDef(version = "1.0", label = "Text file reader", description = "Produces lines from a text file")
public class TextFileReader extends BaseSource {

  @ConfigDef(defaultValue = "", label = "File Location", description = "Absolute file name of the file",
      required = true, type = ConfigDef.Type.STRING)
  public String fileName;

  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = ExtensionsProvider.class)
  @ConfigDef(defaultValue = "", label = "File Extenson", description = "Absolute file name of the file",
     required = true, type = ConfigDef.Type.MODEL)
  public String fileExtension;

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
