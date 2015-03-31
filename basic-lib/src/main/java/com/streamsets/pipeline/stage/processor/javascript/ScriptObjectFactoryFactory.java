package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import javax.script.ScriptEngine;

public class ScriptObjectFactoryFactory {

  private static double JAVA_VERSION = getVersion();

  static double getVersion () {
    String version = System.getProperty("java.version");
    int pos = version.indexOf('.');
    pos = version.indexOf('.', pos+1);
    return Double.parseDouble (version.substring (0, pos));
  }

  public static ScriptObjectFactory getScriptObjectFactory(ScriptEngine engine) {
    double version = getVersion();
    if (version >= 1.8) {
      return new Java8JavaScriptObjectFactory(engine);
    } else {
      return new Java7JavaScriptObjectFactory(engine);
    }
  }
}
