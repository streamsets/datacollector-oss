package com.external.stage;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseProcessor;

/**
 * Created by harikiran on 10/22/14.
 */
@StageDef(name = "TwitterProcessor", description = "processes twitter feeds", label = "twitter_processor"
, version = "1.0")
public class TwitterProcessor extends BaseProcessor{

  @ConfigDef(
    name = "regEx",
    defaultValue = "[a-z][A-Z][0-9]",
    label = "regEx",
    required = true,
    description = "The regular expression used to parse the tweet",
    type = ConfigDef.Type.STRING
  )
  private final String regEx;

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
