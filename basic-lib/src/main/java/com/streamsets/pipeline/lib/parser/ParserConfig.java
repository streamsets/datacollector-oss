package com.streamsets.pipeline.lib.parser;

import java.util.HashMap;
import java.util.Map;

public class ParserConfig {
  Map<ParserConfigKey, Object> configs = new HashMap<>();

  public ParserConfig() {}

  public ParserConfig(ParserConfigKey key, Object value) {
    configs.put(key, value);
  }

  public ParserConfig(Map<ParserConfigKey, Object> other) {
    configs.putAll(other);
  }

  public String getString(ParserConfigKey key) {
    return (String) configs.get(ParserConfigKey.CHARSET);
  }

  public long getLong(ParserConfigKey key) {
    return (long) configs.get(key);
  }

  public int getInteger(ParserConfigKey key) {
    return (int) configs.get(key);
  }

  public Boolean getBoolean(ParserConfigKey key) {
    return (boolean) configs.get(key);
  }

  public void putConfigs(Map<ParserConfigKey, String> other) {
    configs.putAll(other);
  }

  public void setConfigs(Map<ParserConfigKey, String> other) {
    configs.clear();
    putConfigs(other);
  }

  public void removeConfig(ParserConfigKey key) {
    configs.remove(key);
  }

  public void put(ParserConfigKey key, Object value) {
    configs.put(key, value);
  }

}
