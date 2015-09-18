/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  private static abstract class Ref {
    private String unresolvedValue;

    protected Ref(String unresolvedValue) {
      this.unresolvedValue = unresolvedValue;
    }

    public abstract  String getTokenDelimiter();

    protected static boolean isValueMyRef(String  tokenDelimiter, String value) {
      value = value.trim();
      return value.startsWith(tokenDelimiter) && value.endsWith(tokenDelimiter);
    }

    public String getUnresolvedValue() {
      return unresolvedValue;
    }

    protected String getUnresolvedValueWithoutDelimiter() {
      return unresolvedValue.substring(getTokenDelimiter().length(),
                                       unresolvedValue.length() - getTokenDelimiter().length());
    }

    public abstract  String getValue();

    @Override
    public String toString() {
      return Utils.format("{}='{}'", getClass().getSimpleName(), unresolvedValue);
    }

  }

  private static class StringRef extends Ref {

    protected StringRef(String unresolvedValue) {
      super(unresolvedValue);
    }

    @Override
    public String getTokenDelimiter() {
      return "";
    }

    @Override
    public String getValue() {
      return getUnresolvedValue();
    }

  }

  private static class FileRef extends Ref {
    private static final String DELIMITER = "@";

    public static boolean isValueMyRef(String value) {
      return isValueMyRef(DELIMITER, value);
    }

    protected FileRef(String unresolvedValue) {
      super(unresolvedValue);
      Preconditions.checkState(fileRefsBaseDir != null, "fileRefsBaseDir has not been set");
    }

    @Override
    public String getTokenDelimiter() {
      return DELIMITER;
    }

    @Override
    public String getValue() {
      try (BufferedReader br = new BufferedReader(new FileReader(new File(fileRefsBaseDir,
                                                                          getUnresolvedValueWithoutDelimiter())))) {
        return br.readLine();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

  }

  private static class EnvRef extends Ref {
    private static final String DELIMITER = "$";

    public static boolean isValueMyRef(String value) {
      return isValueMyRef(DELIMITER, value);
    }

    protected EnvRef(String unresolvedValue) {
      super(unresolvedValue);
    }

    @Override
    public String getTokenDelimiter() {
      return DELIMITER;
    }

    @Override
    public String getValue() {
      return System.getenv(getUnresolvedValueWithoutDelimiter());
    }
  }

  private static Ref createRef(String value) {
    Ref ref;
    if (FileRef.isValueMyRef(value)) {
      ref = new FileRef(value);
    } else if (EnvRef.isValueMyRef(value)) {
      ref = new EnvRef(value);
    } else {
      ref = new StringRef(value);
    }
    return ref;
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

  public void save(Writer writer) throws IOException {
    Preconditions.checkNotNull(writer, "writer cannot be null");
    Properties props = new Properties();
    for (Map.Entry<String, Ref> entry : map.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue().getUnresolvedValue());
    }
    props.store(writer, "");
    writer.close();
  }

  @Override
  public String toString() {
    return Utils.format("Configuration['{}']", map);
  }

}
