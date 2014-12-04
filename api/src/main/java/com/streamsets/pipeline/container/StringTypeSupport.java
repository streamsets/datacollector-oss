/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

public class StringTypeSupport extends TypeSupport<String> {

  @Override
  public String convert(Object value) {
    return value.toString();
  }

}
