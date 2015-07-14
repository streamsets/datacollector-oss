/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;


import com.streamsets.pipeline.lib.data.DataFactory;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public abstract class DataGeneratorFactory extends DataFactory {

  protected DataGeneratorFactory(Settings settings) {
    super(settings);
  }

  public DataGenerator getGenerator(File file) throws IOException {
    FileOutputStream fileOutputStream = null;
    try {
      fileOutputStream = new FileOutputStream(file);
      return getGenerator(fileOutputStream);
    } catch (IOException ex) {
      IOUtils.closeQuietly(fileOutputStream);
      throw ex;
    }
  }

  public abstract DataGenerator getGenerator(OutputStream os) throws IOException;

  public Writer createWriter(OutputStream os) {
    return new OutputStreamWriter(os, getSettings().getCharset());
  }

}
