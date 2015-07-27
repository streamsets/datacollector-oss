/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.util;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Configuration {
  private static File fileRefsBaseDir;

  public static void setFileRefsBaseDir(File dir) {
    fileRefsBaseDir = dir;
  }

  private interface Ref {
    public String getValue();
    public String getUnresolvedValue();
  }

  private static class StringRef implements Ref {
    private String value;

    public StringRef(String value) {
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public String getUnresolvedValue() {
      return getValue();
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private static class FileRef implements Ref {
    private String file;

    public FileRef(String file) {
      Preconditions.checkState(fileRefsBaseDir != null, "fileRefsBaseDir has not been set");
      this.file = file;
    }

    public String getFile() {
      return file;
    }

    @Override
    public String getValue() {
      try (BufferedReader br = new BufferedReader(new FileReader(new File(fileRefsBaseDir, file)))) {
        return br.readLine();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public String getUnresolvedValue() {
      return "@" + getFile() + "@";
    }

    @Override
    public String toString() {
      return Utils.format("FileRef '{}'", file);
    }

  }

  private Map<String, Ref> map;

  public Configuration() {
    this(new LinkedHashMap<String, Ref>());
  }

  private Configuration(Map<String, Ref> map) {
    this.map = map;
  }

  public Configuration getUnresolvedConfiguration() {
    Map<String, Ref> subSetMap = new LinkedHashMap<>();
    for (Map.Entry<String, Ref> entry : map.entrySet()) {
        subSetMap.put(entry.getKey(), new StringRef(entry.getValue().getUnresolvedValue()));
    }
    return new Configuration(subSetMap);
  }

  public Configuration getSubSetConfiguration(String namePrefix) {
    Preconditions.checkNotNull(namePrefix, "namePrefix cannot be null");
    Map<String, Ref> subSetMap = new LinkedHashMap<>();
    for (Map.Entry<String, Ref> entry : map.entrySet()) {
      if (entry.getKey().startsWith(namePrefix)) {
        subSetMap.put(entry.getKey(), entry.getValue());
      }
    }
    return new Configuration(subSetMap);
  }


  @JsonValue
  public Map<String, String> getValues() {
    Map<String, String> values = new LinkedHashMap<>();
    for (Map.Entry<String, Ref> entry : map.entrySet()) {
      values.put(entry.getKey(), entry.getValue().getValue());
    }
    return values;
  }

  public Set<String> getNames() {
    return new HashSet<>(map.keySet());
  }

  public boolean hasName(String name) {
    Preconditions.checkNotNull(name, "name cannot be null");
    return map.containsKey(name);
  }

  public void set(String name, String value) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(value, "value cannot be null, use unset");
    map.put(name, createRef(value));
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
    return (map.containsKey(name)) ? map.get(name).getValue() : null;
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
      map.put((String) entry.getKey(), createRef((String) entry.getValue()));
    }
    reader.close();
  }

  private Ref createRef(String value) {
    Ref ref;
    if (value.startsWith("@") && value.endsWith("@")) {
      ref = new FileRef(value.substring(1, value.length() -1));
    } else {
      ref = new StringRef(value);
    }
    return ref;
  }

  public void save(Writer writer) throws IOException {
    Preconditions.checkNotNull(writer, "writer cannot be null");
    Properties props = new Properties();
    for (Map.Entry<String, Ref> entry : map.entrySet()) {
      if (entry.getValue() instanceof StringRef) {
        props.setProperty(entry.getKey(), entry.getValue().getValue());
      } else if (entry.getValue() instanceof FileRef) {
        FileRef fileRef = (FileRef) entry.getValue();
        props.setProperty(entry.getKey(), "@" + fileRef.getFile() + "@");
      }
    }
    props.store(writer, "");
    writer.close();
  }

  @Override
  public String toString() {
    return Utils.format("Configuration['{}']", map);
  }

}
