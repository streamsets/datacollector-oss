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
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

public class FileRawSourcePreviewer implements RawSourcePreviewer {

  private String mimeType;

  @ConfigDef(defaultValue = "", description = "The name of the file to preview", label = "File Name",
      required = true, type = ConfigDef.Type.STRING)
  public String fileName;

  @Override
  public Reader preview(int maxLength) {
    Reader reader;
    try {
      reader = new FileReader(fileName);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    return reader;
  }

  @Override
  public String getMimeType() {
    return mimeType;
  }

  @Override
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }

}
