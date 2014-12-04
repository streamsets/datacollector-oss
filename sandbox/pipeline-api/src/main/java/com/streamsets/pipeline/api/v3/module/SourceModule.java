/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.module;

import com.streamsets.pipeline.api.v3.Source;
import com.streamsets.pipeline.api.v3.Source.Context;

public abstract class SourceModule extends BaseModule<Context> implements Source {

}
