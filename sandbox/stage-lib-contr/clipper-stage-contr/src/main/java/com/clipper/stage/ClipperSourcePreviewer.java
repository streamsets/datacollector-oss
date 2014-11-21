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
package com.clipper.stage;

import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

public class ClipperSourcePreviewer implements RawSourcePreviewer {

  private static final String FILE_NAME = "fileName";
  @Override
  public Map<String, String> getParameters() {
    Map<String, String> params = new HashMap<String, String>(1);
    params.put(FILE_NAME, null);
    return params;
  }

  @Override
  public Reader preview(Map<String, String> previewParams, int maxLength) {
    String fileName = previewParams.get(FILE_NAME);
    Reader reader = null;
    try {
      reader = new FileReader(fileName);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    return reader;
  }

  @Override
  public String getMime() {
    return "text/plain";
  }
}
