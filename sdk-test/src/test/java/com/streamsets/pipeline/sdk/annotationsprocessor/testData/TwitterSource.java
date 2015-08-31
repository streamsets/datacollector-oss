/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.List;
@GenerateResourceBundle
@RawSource(rawSourcePreviewer = TwitterRawSourcePreviewer.class)
@StageDef(description = "Produces twitter feeds", label = "twitter_source"
, version = 1)
@ConfigGroups(TwitterSource.TwitterConfigGroups.class)
public class TwitterSource extends BaseSource {

  @FieldSelectorModel
  @ConfigDef(
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the twitter user",
    type = ConfigDef.Type.MODEL,
    group = "USER_INFO"
  )
  public List<String> username;

  @ConfigDef(
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the twitter user",
    type = ConfigDef.Type.STRING,
    group = "USER_INFO"
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

  public enum TwitterConfigGroups implements Label {
    USER_INFO("User Info"),
    OTHER("Other");

    private final String label;

    private TwitterConfigGroups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }
}
