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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.ChooserMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(description = "Produces twitter feeds", label = "twitter_source", version = "1.0")
public class TwitterValuesProviderAndEnum extends BaseSource {

  @FieldValueChooser(type = ChooserMode.PROVIDED, chooserValues = TwitterTypesProvider.class)
  @ConfigDef(
      defaultValue = "admin",
      label = "username",
      required = true,
      description = "The user name of the twitter user",
      type = ConfigDef.Type.MODEL
  )
  public Map<String, String> username;

  @ConfigDef(
      defaultValue = "admin",
      label = "password",
      required = true,
      description = "The password the twitter user",
      type = ConfigDef.Type.STRING
  )
  public String password;

  public TwitterValuesProviderAndEnum() {
  }

  public Map<String, String> getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }

    //Enums
    @GenerateResourceBundle
    public enum ERROR implements ErrorCode {
      E_001("There should be 1 input lane but there are '{}'"),
      E_002("There should be 1 output lane but there are '{}'");

      private final String msg;

      ERROR(String msg) {
        this.msg = msg;
      }

      @Override
      public String getMessage() {
        return msg;
      }

      @Override
      public String getCode() {
        return name();
      }
    }

    public static class TwitterTypesProvider implements ChooserValues {

      @Override
      public List<String> getValues() {
        List<String> values = new ArrayList<String>();
        values.add("INT");
        values.add("STRING");
        values.add("DATE");

        return values;
      }

      @Override
      public List<String> getLabels() {
        List<String> labels = new ArrayList<String>();
        labels.add("integer_value");
        labels.add("string_value");
        labels.add("date_value");

        return labels;
      }
    }

}