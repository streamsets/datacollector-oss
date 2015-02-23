/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

public enum ModelTypeJson {
  FIELD_SELECTOR_MULTI_VALUED,
  FIELD_SELECTOR_SINGLE_VALUED,
  FIELD_VALUE_CHOOSER,
  VALUE_CHOOSER,
  LANE_PREDICATE_MAPPING,
  COMPLEX_FIELD
}