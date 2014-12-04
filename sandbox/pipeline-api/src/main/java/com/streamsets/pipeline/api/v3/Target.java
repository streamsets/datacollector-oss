/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3;

// it must be @Module annotated
public interface Target extends Module<Module.Context> {

  public void write(Batch batch);

}
