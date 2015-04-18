/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;


import com.streamsets.pipeline.lib.data.DataFactory;

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

  public DataGenerator getGenerator(File file) throws IOException, DataGeneratorException {
    FileOutputStream fileOutputStream;
    try {
      fileOutputStream = new FileOutputStream(file);
    } catch (IOException ex) {
      throw new DataGeneratorException(Errors.DATA_GENERATOR_00, file.getAbsolutePath(), ex.getMessage(), ex);
    }
    try {
      return getGenerator(fileOutputStream);
    } catch (DataGeneratorException ex) {
      try {
        fileOutputStream.close();
      } catch (IOException ioEx) {
        //NOP
      }
      throw ex;
    }
  }

  public abstract DataGenerator getGenerator(OutputStream os) throws IOException, DataGeneratorException;

  public Writer createWriter(OutputStream os) {
    return new OutputStreamWriter(os, getSettings().getCharset());
  }

}
