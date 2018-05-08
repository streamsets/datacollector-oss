/*
 * Copyright 2014 American Institute for Computing Education and Research Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Forked from https://github.com/cloudera/cdk/blob/master/cdk-morphlines/
 * cdk-morphlines-core/src/main/java/com/cloudera/cdk/morphline/stdlib/
 * GrokDictionaries.java
 *
 * com.cloudera.cdk.morphline.stdlib.GrokDictionaries was final and could not
 * be extended
 */
package com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.dictionary;

import com.google.common.io.CharStreams;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.exception.GrokCompilationException;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Grok Dictionary
 *
 * @author Israel Ekpo <israel@aicer.org>
 */
public final class GrokDictionary {

  private static final Logger logger = LoggerFactory.getLogger(GrokDictionary.class);

  private final Map<String, String> regexDictionary = new HashMap<String, String>();

  private boolean ready = false;

  public GrokDictionary() {

  }

  public int getDictionarySize() {
    return this.regexDictionary.size();
  }

  /**
   * Returns a duplicate copy of the regex dictionary
   *
   * @return A copy of the dictionary
   */
  public Map<String, String> getRegexDictionary() {
    return new HashMap<String, String>(this.regexDictionary);
  }

  /**
   * Digests all the dictionaries loaded so far
   *
   * @throws GrokCompilationException if there is a problem
   */
  public void bind() {
    digestExpressions();
    ready = (regexDictionary.size() > 0); // Expression have been digested and we have at least one entry
  }

  /**
   * This method throws a run time exception if the bind() method was not called and the dictionary is empty.
   * @throws IllegalStateException
   */
  private void throwErrorIfDictionaryIsNotReady() {

    if (false == ready) {
      throw new IllegalStateException("Dictionary is empty. Please add at least one dictionary and then invoke the bind() method.");
    }
  }

  /**
   * Compiles the expression into a pattern
   *
   * This uses the internal dictionary of named regular expressions
   *
   * @param expression
   * @return The compiled expression
   */
  public Grok compileExpression(final String expression) {

    throwErrorIfDictionaryIsNotReady();

    final String digestedExpression = digestExpressionAux(expression);

    logger.debug("Digested [" + expression + "] into [" + digestedExpression + "] before compilation");
    return new Grok(Pattern.compile(digestedExpression));
  }

  private void digestExpressions() {

    boolean wasModified = true;

    while (wasModified) {

      wasModified = false;

      for(Map.Entry<String, String> entry: regexDictionary.entrySet()) {

        String originalExpression = entry.getValue();
        String digestedExpression = digestExpressionAux(originalExpression);
        wasModified = (!originalExpression.equals(digestedExpression));

        if (wasModified) {
          entry.setValue(digestedExpression);
          break; // stop the for loop
        }
      }
    }
  }

  /**
   * Digests the original expression into a pure named regex
   *
   * @param originalExpression
   * @return The digested expression
   */
  public String digestExpression(String originalExpression) {

    throwErrorIfDictionaryIsNotReady();

    return digestExpressionAux(originalExpression);
  }

  /**
   * Digests the original expression into a pure named regex
   *
   * @param originalExpression
   */
  private String digestExpressionAux(String originalExpression) {

    final String PATTERN_START = "%{";
    final String PATTERN_STOP = "}";
    final char PATTERN_DELIMITER = ':';

    while(true) {

      int PATTERN_START_INDEX = originalExpression.indexOf(PATTERN_START);
      int PATTERN_STOP_INDEX = originalExpression.indexOf(PATTERN_STOP, PATTERN_START_INDEX + PATTERN_START.length());

      // End the loop is %{ or } is not in the current line
      if (PATTERN_START_INDEX < 0 || PATTERN_STOP_INDEX < 0) {
        break;
      }

      // Grab what's inside %{ }
      String grokPattern = originalExpression.substring(PATTERN_START_INDEX + PATTERN_START.length(), PATTERN_STOP_INDEX);

      // Where is the : character
      int PATTERN_DELIMITER_INDEX = grokPattern.indexOf(PATTERN_DELIMITER);

      String regexName = grokPattern;
      String groupName = null;

      if (PATTERN_DELIMITER_INDEX >= 0) {
        regexName = grokPattern.substring(0, PATTERN_DELIMITER_INDEX);
        groupName = grokPattern.substring(PATTERN_DELIMITER_INDEX + 1, grokPattern.length());
      }

      final String dictionaryValue = regexDictionary.get(regexName);

      if (dictionaryValue == null) {
        throw new GrokCompilationException("Missing value for regex name : " + regexName);
      }

      // Defer till next iteration
      if (dictionaryValue.contains(PATTERN_START)) {
        break;
      }

      String replacement = dictionaryValue;

      // Named capture group
      if (null != groupName) {
        replacement = "(?<" + groupName + ">" + dictionaryValue + ")";
      }

      originalExpression = new StringBuilder(originalExpression).replace(PATTERN_START_INDEX, PATTERN_STOP_INDEX + PATTERN_STOP.length(), replacement).toString();
    }

    return originalExpression;
  }

  /**
   * The Grok dictionary file or directory containing the dictionaries
   *
   * @param file The file or directory containing the dictionaries
   *
   * @throws IOException
   */
  public void addDictionary(final File file) {
    try {
      addDictionaryAux(file);
    } catch (IOException e) {
      throw new GrokCompilationException(e);
    }
  }

  /**
   * Loads dictionary from an input stream
   *
   * This can be used to load dictionaries available in the class path <p>
   *
   * @param inputStream
   */
  public void addDictionary(final InputStream inputStream) {

    try {
      addDictionaryAux(new InputStreamReader(inputStream, "UTF-8"));
    } catch (IOException e) {
      throw new GrokCompilationException(e);
    }
  }

  private void addDictionaryAux(final File file) throws IOException {

    if (false == file.exists()) {
      throw new GrokCompilationException("The path specfied could not be found: " + file);
    }

    if (!file.canRead()) {
      throw new GrokCompilationException("The path specified is not readable" + file);
    }

    if (file.isDirectory()) {

      File[] children = file.listFiles();

      // Cycle through the directory and process all child files or folders
      for (File child : children) {
        addDictionaryAux(child);
      }

    } else {

      Reader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");

      try {
        addDictionaryAux(reader);
      } finally {
        IOUtils.closeQuietly(reader);
      }
    }

  }

  /**
   * Adds a dictionary entry via a Reader object
   *
   * @param reader
   */
  public void addDictionary(Reader reader) {

    try {
      addDictionaryAux(reader);
    } catch (IOException e) {
      throw new GrokCompilationException(e);
    } finally {
      IOUtils.closeQuietly(reader);
    }
  }

  private void addDictionaryAux(Reader reader) throws IOException {

    for (String currentFileLine : CharStreams.readLines(reader)) {

      final String currentLine = currentFileLine.trim();

      if (currentLine.length() == 0 || currentLine.startsWith("#")) {
        continue;
      }

      int entryDelimiterPosition = currentLine.indexOf(" ");

      if (entryDelimiterPosition < 0) {
        throw new GrokCompilationException("Dictionary entry (name and value) must be space-delimited: " + currentLine);
      }

      if (entryDelimiterPosition == 0) {
        throw new GrokCompilationException("Dictionary entry must contain a name. " + currentLine);
      }

      final String dictionaryEntryName = currentLine.substring(0, entryDelimiterPosition);
      final String dictionaryEntryValue = currentLine.substring(entryDelimiterPosition + 1, currentLine.length()).trim();

      if (dictionaryEntryValue.length() == 0) {
        throw new GrokCompilationException("Dictionary entry must contain a value: " + currentLine);
      }

      regexDictionary.put(dictionaryEntryName, dictionaryEntryValue);
    }
  }

}