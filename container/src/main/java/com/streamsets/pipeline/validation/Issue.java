/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

public class Issue {
  private final LocalizableString message;

  protected Issue(ErrorCode error, Object... args) {
    message = new ErrorMessage(error, args);
  }

  public Issue(ValidationError error, Object... args) {
    this((ErrorCode) error, args);
  }

  public String getMessage() {
    return message.getLocalized();
  }

  public String toString() {
    return Utils.format("Issue[message='{}']", message.getNonLocalized());
  }

}
