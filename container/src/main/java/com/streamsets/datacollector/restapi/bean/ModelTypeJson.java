/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

public enum ModelTypeJson {
  FIELD_SELECTOR_MULTI_VALUE,
  FIELD_SELECTOR,
  FIELD_VALUE_CHOOSER,
  VALUE_CHOOSER,
  MULTI_VALUE_CHOOSER,
  PREDICATE,
  LIST_BEAN
}