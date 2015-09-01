/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import java.util.Collections;

public enum ModelType {
  FIELD_SELECTOR_MULTI_VALUE(new EmptyListDefaultPreparer()),
  FIELD_SELECTOR(new NopDefaultPreparer()),
  FIELD_VALUE_CHOOSER(new NopDefaultPreparer()),
  VALUE_CHOOSER(new NopDefaultPreparer()),
  MULTI_VALUE_CHOOSER(new NopDefaultPreparer()),
  PREDICATE(new EmptyMapDefaultPreparer()),
  LIST_BEAN(new EmptyListDefaultPreparer()),

  ;

  private final DefaultPreparer defaultPreparer;

  ModelType(DefaultPreparer defaultPreparer) {
    this.defaultPreparer = defaultPreparer;
  }

  public Object prepareDefault(Object defaultValue) {
    return defaultPreparer.prepare(defaultValue);
  }

  private interface DefaultPreparer {
    public Object prepare(Object defaultValue);
  }

  private static class NopDefaultPreparer implements DefaultPreparer {
    @Override
    public Object prepare(Object defaultValue) {
      return defaultValue;
    }
  }

  private static class EmptyListDefaultPreparer implements DefaultPreparer {
    @Override
    public Object prepare(Object defaultValue) {
      return Collections.emptyList();
    }
  }

  private static class EmptyMapDefaultPreparer implements DefaultPreparer {
    @Override
    public Object prepare(Object defaultValue) {
      return Collections.emptyMap();
    }
  }

}
