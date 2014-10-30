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
import com.streamsets.pipeline.api.base.BaseSource;

/**
 * Defines multiple stages as inner class
 */
public class TwitterStages {

  @StageDef(name = "TwitterTarget", description = "Consumes twitter feeds", label = "twitter_target"
    , version = "1.3")
  public class TwitterTarget implements Target {

    @ConfigDef(
      name = "username",
      defaultValue = "admin",
      label = "username",
      required = true,
      description = "The user name of the twitter user",
      type = ConfigDef.Type.STRING
    )
    public String username;

    @ConfigDef(
      name = "password",
      defaultValue = "admin",
      label = "password",
      required = true,
      description = "The password the twitter user",
      type = ConfigDef.Type.STRING
    )
    public String password;

    public TwitterTarget() {
    }

    public TwitterTarget(String username, String password) {
      this.username = username;
      this.password = password;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public void write(Batch batch) throws StageException {

    }

    @Override
    public void init(Info info, Context context) throws StageException {

    }

    @Override
    public void destroy() {

    }
  }

  @StageDef(name = "TwitterSource", description = "Produces twitter feeds", label = "twitter_source"
    , version = "1.0")
  public class TwitterSource extends BaseSource {

    @ConfigDef(
      name = "username",
      defaultValue = "admin",
      label = "username",
      required = true,
      description = "The user name of the twitter user",
      type = ConfigDef.Type.STRING
    )
    public String username;

    @ConfigDef(
      name = "password",
      defaultValue = "admin",
      label = "password",
      required = true,
      description = "The password the twitter user",
      type = ConfigDef.Type.STRING
    )
    public String password;

    public TwitterSource() {
    }

    public TwitterSource(String username, String password) {
      this.username = username;
      this.password = password;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public String produce(String lastSourceOffset, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @StageDef(name = "TwitterProcessor", description = "processes twitter feeds", label = "twitter_processor"
    , version = "1.0")
  public class TwitterProcessor extends BaseProcessor {

    @ConfigDef(
      name = "regEx",
      defaultValue = "[a-z][A-Z][0-9]",
      label = "regEx",
      required = true,
      description = "The regular expression used to parse the tweet",
      type = ConfigDef.Type.STRING
    )
    public String regEx;

    public TwitterProcessor() {
    }

    public TwitterProcessor(String username, String password) {
      this.regEx = username;

    }

    public String getRegEx() {
      return regEx;
    }


    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {

    }
  }

  @StageErrorDef
  public enum TwitterError implements ErrorId {
    // We have an trailing whitespace for testing purposes
    INPUT_LANE_ERROR("There should be 1 input lane but there are '{}' "),
    OUTPUT_LANE_ERROR("There should be 1 output lane but there are '{}' ");

    private String msg;

    TwitterError(String msg) {
      this.msg = msg;
    }

    @Override
    public String getMessageTemplate() {
      return msg;
    }
  }
}
