/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.json.ObjectMapperFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Provides methods to read, write and append objects from and to files.
 * @param <T> the type of the object that needs to be persisted or retrieved
 */
public class JsonFileUtil<T> {

  private ObjectMapper json;

  public JsonFileUtil() {
    json = ObjectMapperFactory.get();
  }

  public void writeObjectToFile(File tempFile, File originalFile, T obj) throws IOException {
    json.writeValue(tempFile, obj);
    Files.move(tempFile.toPath(), originalFile.toPath(), StandardCopyOption.ATOMIC_MOVE
        , StandardCopyOption.REPLACE_EXISTING);
  }

  public void appendObjectToFile(File tempFile, File originalFile, T obj) throws IOException {
    if(originalFile.exists()) {
      Files.copy(originalFile.toPath(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
    json.writeValue(new FileOutputStream(tempFile, true), obj);
    Files.move(tempFile.toPath(), originalFile
        .toPath(), StandardCopyOption.ATOMIC_MOVE
        , StandardCopyOption.REPLACE_EXISTING);
  }

  public T readObjectFromFile(File file, Class<T> klass) throws IOException {
    return json.readValue(file, klass);
  }

}
