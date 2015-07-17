/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Defines multiple stages as inner class
 */
public class TwitterStages {

  @StageDef(description = "Produces twitter feeds", label = "twitter_source"
    , version = 1)
  public class TwitterSource extends BaseSource{

    @FieldSelector
    @ConfigDef(
      defaultValue = "admin",
      label = "username",
      required = true,
      description = "The user name of the twitter user",
      type = ConfigDef.Type.MODEL
    )
    public List<String> username;

    @ConfigDef(
      defaultValue = "admin",
      label = "password",
      required = true,
      description = "The password the twitter user",
      type = ConfigDef.Type.STRING
    )
    public String password;

    public TwitterSource() {
    }

    public List<String> getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @StageDef(description = "processes twitter feeds", label = "twitter_processor"
    , version = 1)
  public class TwitterProcessor extends BaseProcessor {

    @FieldValueChooser(TypesProvider.class)
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


  @StageDef(description = "Consumes twitter feeds", label = "twitter_target"
    , version = 3)
  public class TwitterTarget implements Target {

    @ConfigDef(
      defaultValue = "admin",
      label = "username",
      required = true,
      description = "The user name of the twitter user",
      type = ConfigDef.Type.STRING
    )
    public String username;

    @ConfigDef(
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
    public List<ConfigIssue> init(Info info, Context context) {
      return Collections.emptyList();
    }

    @Override
    public void destroy() {

    }

  }

  public enum ERROR implements ErrorCode {
    INPUT_LANE_ERROR("There should be 1 input lane but there are '{}'"),
    OUTPUT_LANE_ERROR("There should be 1 output lane but there are '{}'");
    private final String msg;

    ERROR(String msg) {
      this.msg = msg;
    }


    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return msg;
    }
  }

}
