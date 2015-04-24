/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.el;

public interface ELVars {

  public Object getConstant(String name);

  public void addVariable(String name, Object value);

  public void addContextVariable(String name, Object value);

  public boolean hasVariable(String name);

  public Object getVariable(String name);

  public boolean hasContextVariable(String name);

  public Object getContextVariable(String name);
}
