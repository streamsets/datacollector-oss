/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.data;

import java.util.Map;
import java.util.Set;

public interface DataFormat<F extends DataFactory> {

  public Set<Class<? extends Enum>> getModes();

  public Map<String, Object> getConfigs();

  public F create(DataFactory.Settings settings);
}
