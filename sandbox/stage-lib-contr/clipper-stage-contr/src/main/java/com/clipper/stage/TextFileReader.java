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

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.FieldSelectionType;


@RawSource(rawSourcePreviewer = ClipperSourcePreviewer.class, mimeType = "text/plain")
@StageDef(version = "1.0", label = "Text file reader", description = "Produces lines from a text file")
public class TextFileReader extends BaseSource {

  @ConfigDef(defaultValue = "", label = "File Location", description = "Absolute file name of the file",
      required = true, type = ConfigDef.Type.STRING)
  public String fileName;

  @DropDown(type = FieldSelectionType.PROVIDED, valuesProvider = ExtensionsProvider.class)
  @ConfigDef(defaultValue = "", label = "File Extenson", description = "Absolute file name of the file",
     required = true, type = ConfigDef.Type.MODEL)
  public String fileExtension;

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
