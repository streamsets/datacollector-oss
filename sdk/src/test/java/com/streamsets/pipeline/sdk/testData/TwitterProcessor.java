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
package com.streamsets.pipeline.sdk.testData;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.ChooserMode;

import java.util.Map;
@GenerateResourceBundle
@StageDef(description = "processes twitter feeds", label = "twitter_processor"
, version = "1.0")
public class TwitterProcessor extends BaseProcessor{

  @FieldValueChooser(type = ChooserMode.PROVIDED, chooserValues = TypesProvider.class)
  @ConfigDef(
    defaultValue = "[a-z][A-Z][0-9]",
    label = "regEx",
    required = true,
    description = "The regular expression used to parse the tweet",
    type = ConfigDef.Type.MODEL
  )
  public Map<String, String> regEx;


  public TwitterProcessor() {
  }

  public TwitterProcessor(Map<String, String> regEx) {
    this.regEx = regEx;

  }

  public Map<String, String> getRegEx() {
    return regEx;
  }


  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {

  }
}
