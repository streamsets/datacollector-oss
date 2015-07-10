/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import java.util.Collections;

public enum ModelType {
  FIELD_SELECTOR_MULTI_VALUED(new EmptyListDefaultPreparer()),
  FIELD_SELECTOR_SINGLE_VALUED(new NopDefaultPreparer()),
  FIELD_VALUE_CHOOSER(new NopDefaultPreparer()),
  VALUE_CHOOSER(new NopDefaultPreparer()),
  LANE_PREDICATE_MAPPING(new EmptyMapDefaultPreparer()),
  COMPLEX_FIELD(new EmptyListDefaultPreparer()),

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
