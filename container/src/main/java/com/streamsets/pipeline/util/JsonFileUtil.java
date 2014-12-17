/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.json.ObjectMapperFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class JsonFileUtil {

  public static void writeObjectToFile(File tempFile, File originalFile, Object obj) throws IOException {
    ObjectMapperFactory.get().writeValue(tempFile, obj);
    Files.move(tempFile.toPath(), originalFile.toPath(), StandardCopyOption.ATOMIC_MOVE
        , StandardCopyOption.REPLACE_EXISTING);
  }

  public static void appendObjectToFile(File tempFile, File originalFile, Object obj) throws IOException {
    if(originalFile.exists()) {
      Files.copy(originalFile.toPath(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
    ObjectMapperFactory.get().writeValue(new FileOutputStream(tempFile, true), obj);
    Files.move(tempFile.toPath(), originalFile
        .toPath(), StandardCopyOption.ATOMIC_MOVE
        , StandardCopyOption.REPLACE_EXISTING);
  }

  public static Object readObjectFromFile(File file, Class klass) throws IOException {
    return ObjectMapperFactory.get().readValue(file, klass);
  }
}
