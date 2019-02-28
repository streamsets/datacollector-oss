/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.dictionary.GrokDictionary;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class LogDataParserFactory extends DataParserFactory {

  static final String KEY_PREFIX = "log.";
  public static final String RETAIN_ORIGINAL_TEXT_KEY = KEY_PREFIX + "retain.original.text";
  static final boolean RETAIN_ORIGINAL_TEXT_DEFAULT = false;
  public static final String APACHE_CUSTOMLOG_FORMAT_KEY = KEY_PREFIX + "apache.custom.log.format";
  static final String APACHE_CUSTOMLOG_FORMAT_DEFAULT = "%h %l %u %t \"%r\" %>s %b";
  public static final String REGEX_KEY = KEY_PREFIX + "regex";
  static final String REGEX_DEFAULT =
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
  public static final String REGEX_FIELD_PATH_TO_GROUP_KEY = KEY_PREFIX + "regex.fieldPath.to.group.name";
  static final Map<String, Integer> REGEX_FIELD_PATH_TO_GROUP_DEFAULT = new HashMap<>();
  public static final String GROK_PATTERN_KEY = KEY_PREFIX + "grok.patterns";
  static final String GROK_PATTERN_DEFAULT = "%{COMMONAPACHELOG}";
  public static final String GROK_PATTERN_DEFINITION_KEY = KEY_PREFIX + "grok.pattern.definition";
  static final String GROK_PATTERN_DEFINITION_DEFAULT = "";
  public static final String LOG4J_FORMAT_KEY = KEY_PREFIX + "log4j.custom.log.format";
  static final String LOG4J_FORMAT_DEFAULT = "%d{ISO8601} %-5p %c{1} - %m";

  public static final String ON_PARSE_ERROR_KEY = KEY_PREFIX + "on.parse.error";
  public static final OnParseError ON_PARSE_ERROR_DEFAULT = OnParseError.ERROR;
  public static final String LOG4J_TRIM_STACK_TRACES_TO_LENGTH_KEY = KEY_PREFIX + "log4j.trim.stack.trace.to.length";
  static final int LOG4J_TRIM_STACK_TRACES_TO_LENGTH_DEFAULT = 50;

  public static final String MULTI_LINES_KEY = "multiLines";
  public static final Boolean MULTI_LINES_DEFAULT = false;

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(RETAIN_ORIGINAL_TEXT_KEY, RETAIN_ORIGINAL_TEXT_DEFAULT);
    configs.put(APACHE_CUSTOMLOG_FORMAT_KEY, APACHE_CUSTOMLOG_FORMAT_DEFAULT);
    configs.put(REGEX_KEY, REGEX_DEFAULT);
    configs.put(REGEX_FIELD_PATH_TO_GROUP_KEY, REGEX_FIELD_PATH_TO_GROUP_DEFAULT);
    configs.put(GROK_PATTERN_DEFINITION_KEY, GROK_PATTERN_DEFINITION_DEFAULT);
    configs.put(GROK_PATTERN_KEY, Arrays.asList(GROK_PATTERN_DEFAULT));
    configs.put(LOG4J_FORMAT_KEY, LOG4J_FORMAT_DEFAULT);
    configs.put(ON_PARSE_ERROR_KEY, ON_PARSE_ERROR_DEFAULT);
    configs.put(LOG4J_TRIM_STACK_TRACES_TO_LENGTH_KEY, LOG4J_TRIM_STACK_TRACES_TO_LENGTH_DEFAULT);
    configs.put(MULTI_LINES_KEY, MULTI_LINES_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }


  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(LogMode.class);


  private final ProtoConfigurableEntity.Context context;
  private final int maxObjectLen;
  private final LogMode logMode;
  private final boolean retainOriginalText;
  private final String customLogFormat;
  private final String regex;
  private final Map<String, Integer> fieldPathToGroup;
  private final String grokPatternDefinition;
  private final List<String> grokPatternList;
  private final List<String> grokDictionaries;
  private final String log4jCustomLogFormat;
  private final OnParseError onParseError;
  private final int maxStackTraceLength;
  private final Map<String, Object> regexToPatternMap;
  private final GenericObjectPool<StringBuilder> currentLineBuilderPool;
  private final GenericObjectPool<StringBuilder> previousLineBuilderPool;

  public LogDataParserFactory(Settings settings) {
    super(settings);
    this.context = settings.getContext();
    this.maxObjectLen = settings.getMaxRecordLen();
    this.logMode = settings.getMode(LogMode.class);
    this.retainOriginalText = settings.getConfig(RETAIN_ORIGINAL_TEXT_KEY);
    this.customLogFormat = settings.getConfig(APACHE_CUSTOMLOG_FORMAT_KEY);
    this.regex = settings.getConfig(REGEX_KEY);
    this.fieldPathToGroup = settings.getConfig(REGEX_FIELD_PATH_TO_GROUP_KEY);
    this.grokPatternDefinition = settings.getConfig(GROK_PATTERN_DEFINITION_KEY);
    this.grokPatternList = settings.getConfig(GROK_PATTERN_KEY);
    this.grokDictionaries = Collections.emptyList();
    this.log4jCustomLogFormat = settings.getConfig(LOG4J_FORMAT_KEY);
    this.onParseError = settings.getConfig(ON_PARSE_ERROR_KEY);
    this.maxStackTraceLength = settings.getConfig(LOG4J_TRIM_STACK_TRACES_TO_LENGTH_KEY);
    this.regexToPatternMap = new HashMap<>();
    this.currentLineBuilderPool = getStringBuilderPool(settings);
    this.previousLineBuilderPool = getStringBuilderPool(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return createParser(id, createReader(is), Long.parseLong(offset));
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return createParser(id, createReader(reader), offset);
  }

  @Override
  public void destroy() {
    if (previousLineBuilderPool != null) {
      previousLineBuilderPool.close();
    }

    if (currentLineBuilderPool != null) {
      currentLineBuilderPool.close();
    }

    super.destroy();
  }

  private DataParser createParser(String id, OverrunReader reader, long offset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      switch (logMode) {
        case COMMON_LOG_FORMAT:
          return new GrokParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            getMaxStackTraceLines(), createGroks(Arrays.asList(Constants.GROK_COMMON_APACHE_LOG_FORMAT),
            Collections.<String>emptyList()), "Common Log Format", currentLineBuilderPool, previousLineBuilderPool);
        case COMBINED_LOG_FORMAT:
          return new GrokParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            getMaxStackTraceLines(), createGroks(Arrays.asList(Constants.GROK_COMBINED_APACHE_LOG_FORMAT),
            Collections.<String>emptyList()), "Combined Log Format", currentLineBuilderPool, previousLineBuilderPool);
        case APACHE_CUSTOM_LOG_FORMAT:
          return new GrokParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            getMaxStackTraceLines(), createGroks(Arrays.asList(ApacheCustomLogHelper.translateApacheLayoutToGrok(customLogFormat)),
            Collections.<String>emptyList()), "Apache Access Log Format", currentLineBuilderPool, previousLineBuilderPool);
        case APACHE_ERROR_LOG_FORMAT:
          return new GrokParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            getMaxStackTraceLines(), createGroks(Arrays.asList(Constants.GROK_APACHE_ERROR_LOG_FORMAT),
            ImmutableList.of(Constants.GROK_APACHE_ERROR_LOG_PATTERNS_FILE_NAME)), "Apache Error Log Format",
            currentLineBuilderPool, previousLineBuilderPool);
        case REGEX:
          return new RegexParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            createPattern(regex), fieldPathToGroup, currentLineBuilderPool, previousLineBuilderPool);
        case GROK:
          return new GrokParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            getMaxStackTraceLines(), createGroks(grokPatternList, grokDictionaries), "Grok Format",
            currentLineBuilderPool, previousLineBuilderPool);
        case LOG4J:
          return new GrokParser(context, id, reader, offset, maxObjectLen, retainOriginalText,
            getMaxStackTraceLines(), createGroks(Arrays.asList(Log4jHelper.translateLog4jLayoutToGrok(log4jCustomLogFormat)),
            ImmutableList.of(Constants.GROK_LOG4J_LOG_PATTERNS_FILE_NAME)),
            "Log4j Log Format", currentLineBuilderPool, previousLineBuilderPool);
        case CEF:
          return new CEFParser(
              context,
              id,
              reader,
              offset,
              maxObjectLen,
              retainOriginalText,
              currentLineBuilderPool,
              previousLineBuilderPool
          );
        case LEEF:
          return new LEEFParser(
              context,
              id,
              reader,
              offset,
              maxObjectLen,
              retainOriginalText,
              currentLineBuilderPool,
              previousLineBuilderPool
          );
        default:
          return null;
      }
    } catch (IOException ex) {
      throw new DataParserException(Errors.LOG_PARSER_00, id, offset, ex.toString(), ex);
    }
  }

  @VisibleForTesting
  private List<Grok> createGroks(List<String> grokPatternList, List<String> dictionaries) {
    List<Grok> grokList = new ArrayList<>();
    List<String> grokPatternsToProcess = new ArrayList<>();

    grokPatternList.forEach(grokPatternItem -> {
      if(regexToPatternMap.containsKey(grokPatternItem)) {
        grokList.add((Grok) regexToPatternMap.get(grokPatternItem));
      } else {
        grokPatternsToProcess.add(grokPatternItem);
      }
    });

    // directly return if all groks where previously built and stored in regexToPatternMap
    if (grokPatternsToProcess.isEmpty()) {
      return grokList;
    }

    GrokDictionary grokDictionary = new GrokDictionary();
    //Add grok patterns and Java patterns by default
    try(
      InputStream grogPatterns = getClass().getClassLoader().getResourceAsStream(Constants.GROK_PATTERNS_FILE_NAME);
      InputStream javaPatterns = getClass().getClassLoader().getResourceAsStream(Constants.GROK_JAVA_LOG_PATTERNS_FILE_NAME);
    ) {
      grokDictionary.addDictionary(grogPatterns);
      grokDictionary.addDictionary(javaPatterns);
      for(String dictionary : dictionaries) {
        try(InputStream dictionaryStream = getClass().getClassLoader().getResourceAsStream(dictionary)) {
          grokDictionary.addDictionary(dictionaryStream);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't close resource stream", e);
    }
    if(grokPatternDefinition != null && !grokPatternDefinition.isEmpty()) {
      grokDictionary.addDictionary(new StringReader(grokPatternDefinition));
    }
    // Resolve all expressions loaded
    grokDictionary.bind();

    grokPatternsToProcess.forEach(grokPatternItem -> {
      Grok grok = grokDictionary.compileExpression(grokPatternItem);
      regexToPatternMap.put(grokPatternItem, grok);
      grokList.add(grok);
    });

    return grokList;
  }

  @VisibleForTesting
  private Pattern createPattern(String regex) {
    if(regexToPatternMap.containsKey(regex)) {
      return (Pattern) regexToPatternMap.get(regex);
    }
    Pattern pattern = Pattern.compile(regex);
    regexToPatternMap.put(regex, pattern);
    return pattern;
  }

  public int getMaxStackTraceLines() {
    switch (onParseError) {
      case ERROR:
        return -1;
      case IGNORE:
        return 0;
      case INCLUDE_AS_STACK_TRACE:
        return maxStackTraceLength;
      default:
        throw new IllegalArgumentException("Unexpected value for OnParseError");
    }
  }

  @VisibleForTesting
  GenericObjectPool<StringBuilder> getCurrentLineBuilderPool() {
    return currentLineBuilderPool;
  }

  @VisibleForTesting
  GenericObjectPool<StringBuilder> getPreviousLineBuilderPool() {
    return previousLineBuilderPool;
  }
}
