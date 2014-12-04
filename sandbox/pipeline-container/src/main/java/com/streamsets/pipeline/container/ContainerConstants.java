/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public interface ContainerConstants {

  public static final Set<String> INVALID_MODULE_NAMES = ImmutableSet.of("pipeline");

  public static final String INVALID_MODULE_CHARACTERS = ":()";

}
