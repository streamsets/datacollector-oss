/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;


import com.streamsets.pipeline.lib.data.DataFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public abstract class CharDataGeneratorFactory extends DataFactory {

  protected CharDataGeneratorFactory(Settings settings) {
    super(settings);
  }

  public DataGenerator getGenerator(File file) throws IOException, DataGeneratorException {
    FileWriter fileWriter;
    try {
      fileWriter = new FileWriter(file);
    } catch (IOException ex) {
      throw new DataGeneratorException(Errors.DATA_GENERATOR_00, file.getAbsolutePath(), ex.getMessage(), ex);
    }
    try {
      return getGenerator(fileWriter);
    } catch (DataGeneratorException ex) {
      try {
        fileWriter.close();
      } catch (IOException ioEx) {
        //NOP
      }
      throw ex;
    }
  }

  public abstract DataGenerator getGenerator(Writer writer) throws IOException, DataGeneratorException;

}
