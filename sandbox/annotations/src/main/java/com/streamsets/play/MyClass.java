/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;


@Module(name="foo", version="0.1.0", type= Module.Type.PROCESSOR)
public class MyClass {

  @Configuration(name="name", type= Configuration.Type.STRING, defaultValue="foo")
  private String name;

  @Configuration(name="windowSize", type=Configuration.Type.INTEGER, defaultValue="1000")
  private Integer windowSize;

  @Configuration(name="timeInterval", type=Configuration.Type.LONG)
  private Integer timeInterval;

  @Configuration(name="debug", type=Configuration.Type.BOOLEAN, defaultValue = "true")
  private Integer debug;


}

