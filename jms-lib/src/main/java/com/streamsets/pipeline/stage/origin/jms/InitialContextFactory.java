/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;


public class InitialContextFactory {

  public InitialContext create(Properties properties) throws NamingException {
    return new InitialContext(properties);
  }
}