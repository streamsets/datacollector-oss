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
package com.streamsets.pipeline.lib.stage.source.spooldir.json;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.stage.source.spooldir.AbstractSpoolDirSource;

import java.io.File;

@StageDef(name = "jsonSpoolDirectory",
    version = "1.0.0",
    label = "JSON files spool directory",
    description = "Consumes JSON files from a spool directory")
public class JsonSpoolDirSource extends AbstractSpoolDirSource {

  @ConfigDef(name="jsonContent",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JSON Content",
      description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects, " +
                    "ARRAY_OF_OBJECTS or MULTIPLE_OBJECTS",
      defaultValue = "ARRAY_OF_OBJECTS")
  public String jsonContent;

  @ConfigDef(name="jsonObjectType",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JSON Object Type",
      description = "Indicates the type of the JSON objects, MAP or ARRAY",
      defaultValue = "MAP")
  public String jsonObjectType;

  @Override
  protected long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return 0;
  }

}
