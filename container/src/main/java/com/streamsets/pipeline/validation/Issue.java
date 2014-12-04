/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.container.ErrorMessage;
import com.streamsets.pipeline.container.LocalizableString;
import com.streamsets.pipeline.container.Utils;

public class Issue {
  private final LocalizableString message;

  public Issue(ValidationError error, Object... args) {
    message = new ErrorMessage(error, args);
  }

  public String getMessage() {
    return message.getLocalized();
  }

  public String toString() {
    return Utils.format("Issue[message='{}']", message.getNonLocalized());
  }

}
