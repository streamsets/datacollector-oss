/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.container.Utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Configuration {
  private Map<String, String> map;

  public Configuration() {
    map = new LinkedHashMap<String, String>();
  }

  public Configuration getSubSetConfiguration(String namePrefix) {
    Preconditions.checkNotNull(namePrefix, "namePrefix cannot be null");
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey().startsWith(namePrefix)) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }


  @JsonValue
  public Map<String, String> getValues() {
    return map;
  }

  public Set<String> getNames() {
    return new HashSet<String>(map.keySet());
  }

  public boolean hasName(String name) {
    Preconditions.checkNotNull(name, "name cannot be null");
    return map.containsKey(name);
  }

  public void set(String name, String value) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(value, "value cannot be null, use unset");
    map.put(name, value);
  }

  public void unset(String name) {
    Preconditions.checkNotNull(name, "name cannot be null");
    map.remove(name);
  }

  public void set(String name, int value) {
    set(name, Integer.toString(value));
  }

  public void set(String name, long value) {
    set(name, Long.toString(value));
  }

  public void set(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  private String get(String name) {
    Preconditions.checkNotNull(name, "name cannot be null");
    return map.get(name);
  }

  public String get(String name, String defaultValue) {
    String value = get(name);
    return (value != null) ? value : defaultValue;
  }

  public long get(String name, long defaultValue) {
    String value = get(name);
    return (value != null) ? Long.parseLong(value) : defaultValue;
  }

  public int get(String name, int defaultValue) {
    String value = get(name);
    return (value != null) ? Integer.parseInt(value) : defaultValue;
  }

  public boolean get(String name, boolean defaultValue) {
    String value = get(name);
    return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
  }

  public void load(Reader reader) throws IOException {
    Preconditions.checkNotNull(reader, "reader cannot be null");
    Properties props = new Properties();
    props.load(reader);
    for (Map.Entry entry : props.entrySet()) {
      set((String) entry.getKey(), (String) entry.getValue());
    }
    reader.close();
  }

  public void save(Writer writer) throws IOException {
    Preconditions.checkNotNull(writer, "writer cannot be null");
    Properties props = new Properties();
    props.putAll(map);
    props.store(writer, "");
    writer.close();
  }

  @Override
  public String toString() {
    return Utils.format("Configuration['{}']", map);
  }

}
