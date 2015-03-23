/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(description = "Produces twitter feeds", label = "twitter_source", version = "1.0")
public class TwitterValuesProviderAndEnum extends BaseSource {

  @FieldValueChooser(Misc.TwitterTypesProvider.class)
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

  public static class Misc {

    //Enums
    @GenerateResourceBundle
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

    public static class TwitterTypesProvider implements ChooserValues {

      @Override
      public String getResourceBundle() {
        return null;
      }

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
}