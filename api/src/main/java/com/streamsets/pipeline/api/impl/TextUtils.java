/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import java.util.regex.Pattern;

public class TextUtils {

  public static final String VALID_NAME= "[0-9A-Za-z_\\s]+";

  private static final Pattern VALID_NAME_PATTERN = Pattern.compile(VALID_NAME);

  public static boolean isValidName(String name) {
    return (name != null) && VALID_NAME_PATTERN.matcher(name).matches();
  }

}
