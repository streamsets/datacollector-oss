/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class CharacterField extends Field<Character> {

  public CharacterField(char value) {
    super(Type.CHARACTER, value, true, null);
  }

  public CharacterField(String value) {
    super(Type.CHARACTER, value.charAt(0), true, value);
  }
}
