/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class StringField extends Field<String> {

  public StringField(String value) {
    super(Type.STRING, value, true, value);
  }

}
