/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
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

  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TweetTypeProvider.class)
  @ConfigDef(
      defaultValue = "[a-z][A-Z][0-9]",
      label = "tweetType1",
      required = true,
      description = "The regular expression used to parse the tweet",
      type = ConfigDef.Type.MODEL
  )
  public TweetType tweetType1;

  @FieldValueChooser(type = ChooserMode.PROVIDED, chooserValues = TweetTypeProvider.class)
  @ConfigDef(
      defaultValue = "[a-z][A-Z][0-9]",
      label = "tweetType2",
      required = true,
      description = "The regular expression used to parse the tweet",
      type = ConfigDef.Type.MODEL,
      dependsOn = "tweetType1",
      triggeredByValue = {"a", "b", "c"}
  )
  public Map<String, TweetType> tweetType2;

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
