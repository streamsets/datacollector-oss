/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.domainserver.main;

import com.streamsets.pipeline.main.Main;

public class DomainServerMain extends Main {

  public DomainServerMain() {
    super(DomainServerTaskModule.class);
  }

  public static void main(String[] args) throws Exception {
    System.exit(new DomainServerMain().doMain());
  }
}
