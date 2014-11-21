/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

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
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
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
