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
package org.cloudera.log4j.redactor;

// This file was copied from Cloudera's log redactor project
// https://github.com/cloudera/logredactor

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains the logic for redacting Strings.  It is initialized
 * from rules contained in a JSON file.
 */
public class StringRedactor {

  private RedactionPolicy policy;

  // Prevent use of normal constructor
  private StringRedactor() {}

  /**
   * This class is created by the JSON ObjectMapper in createFromJsonFile().
   * It holds one rule for redaction - a description and then
   * trigger-search-replace. See the comments in createFromJsonFile().
   * Since we only read from JSON files, we only need setter methods.
   */
  private static class RedactionRule {
    private String description;
    private boolean caseSensitive = true;
    private String trigger;
    private String search;
    private String replace;
    private Pattern pattern;
    private ThreadLocal<Matcher> matcherTL;

    public void setDescription(String description) {
      this.description = description;
    }

    public void setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
    }

    public void setTrigger(String trigger) {
      this.trigger = trigger;
    }

    public void setSearch(String search) {
      this.search = search;
      // We create a Pattern here to ensure it's a valid regex. We don't
      // set this.pattern because we don't know yet if it's case
      // sensitive or not. That's done in postProcess().
      Pattern thrownAway = Pattern.compile(search);
    }

    public void setReplace(String replace) {
      this.replace = replace;
    }

    private void postProcess() throws JsonMappingException {
      if ((search == null) || search.isEmpty()) {
        throw new JsonMappingException("The search regular expression cannot " +
            "be empty.");
      }
      if ((replace == null || replace.isEmpty())) {
        throw new JsonMappingException("The replacement text cannot " +
            "be empty.");
      }

      if (caseSensitive) {
        pattern = Pattern.compile(search);
      } else {
        pattern = Pattern.compile(search, Pattern.CASE_INSENSITIVE);
      }
      matcherTL = new ThreadLocal<Matcher>() {
        @Override
        protected Matcher initialValue() {
          return pattern.matcher("");
        }
      };

      // Actually try a sample search-replace with the search and replace.
      // We know the search is valid from the above, but the replace could
      // be malformed - for example $% is an illegal group reference.
      try {
        String sampleString = "Hello, world";
        Matcher m = pattern.matcher(sampleString);
        sampleString = m.replaceAll(replace);
      } catch (Exception e) {
        throw new JsonMappingException("The replacement text \"" + replace +
            "\" is invalid", e);

      }
    }

    private boolean matchesTrigger(String msg) {
      // The common case: an empty trigger.
      if ((trigger == null) || trigger.isEmpty()) {
        return true;
      }

      /* TODO Consider Boyer-More for performance.
       * http://www.cs.utexas.edu/users/moore/publications/fstrpos.pdf
       * However, it might not matter much in our use case.
       */
      if (caseSensitive) {
        return msg.contains(trigger);
      }

      // As there is no case-insensitive contains(), our options are to
      // tolower() the strings (creates and throws away objects), use a regex
      // (slow) or write our own using regionMatches(). We take the latter
      // option, as it's fast.
      final int len = trigger.length();
      final int max = msg.length() - len;
      for (int i = 0; i <= max; i++) {
        if (msg.regionMatches(true, i, trigger, 0, len)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * This class is created by the JSON ObjectMapper in createFromJsonFile().
   * It contains a version number and an array of RedactionRules.
   */
  private static class RedactionPolicy {
    private int version = -1;
    private List<RedactionRule> rules;

    private static RedactionPolicy emptyRedactionPolicy() {
      RedactionPolicy policy = new RedactionPolicy();
      policy.version = 1;
      policy.rules = new ArrayList<RedactionRule>();
      return policy;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    public void setRules(List<RedactionRule> rules) {
      this.rules = rules;
    }

    /**
     * Perform validation checking on the fully constructed JSON, and
     * sets up internal data structures.
     * @throws JsonMappingException
     */
    private void postProcess() throws JsonMappingException {
      if (version == -1) {
        throw new JsonMappingException("No version specified.");
      } else if (version != 1) {
        throw new JsonMappingException("Unknown version " + version);
      }
      for (RedactionRule rule : rules) {
        rule.postProcess();
      }
    }

    /**
     * The actual work of redaction.
     * @param msg The string to redact
     * @return If any redaction was performed, the redacted string. Otherwise
     *         the original is returned.
     */
    private String redact(String msg) {
      if (msg == null) {
        return null;
      }
      String original = msg;
      boolean matched = false;
      for (RedactionRule rule : rules) {
        if (rule.matchesTrigger(msg)) {
          Matcher m = rule.matcherTL.get();
          m.reset(msg);
          if (m.find()) {
            msg = m.replaceAll(rule.replace);
            matched = true;
          }
        }
      }
      return matched ? msg : original;
    }
  }

  /**
   * Create a StringRedactor based on the JSON found in a file. The file
   * format looks like this:
   * {
   *   "version": 1,
   *   "rules": [
   *     { "description": "This is the first rule",
   *       "trigger": "triggerstring 1",
   *       "search": "regex 1",
   *       "replace": "replace 1"
   *     },
   *     { "description": "This is the second rule",
   *       "trigger": "triggerstring 2",
   *       "search": "regex 2",
   *       "replace": "replace 2"
   *     }
   *   ]
   * }
   * @param fileName The name of the file to read
   * @return A freshly allocated StringRedactor
   * @throws JsonParseException, JsonMappingException, IOException
   */
  public static StringRedactor createFromJsonFile(String fileName)
          throws IOException {
    StringRedactor sr = new StringRedactor();
    if (fileName == null) {
      sr.policy = RedactionPolicy.emptyRedactionPolicy();
      return sr;
    }
    File file = new File(fileName);
    // An empty file is explicitly allowed as "no rules"
    if (file.exists() && file.length() == 0) {
      sr.policy = RedactionPolicy.emptyRedactionPolicy();
      return sr;
    }

    ObjectMapper mapper = new ObjectMapper();
    RedactionPolicy policy = mapper.readValue(file, RedactionPolicy.class);
    policy.postProcess();
    sr.policy = policy;
    return sr;
  }

  /**
   * Create a StringRedactor based on the JSON found in the given String.
   * The format is identical to that described in createFromJsonFile().
   * @param json String containing json formatted rules.
   * @return A freshly allocated StringRedactor
   * @throws JsonParseException, JsonMappingException, IOException
   */
  public static StringRedactor createFromJsonString(String json)
          throws IOException {
    StringRedactor sr = new StringRedactor();
    if ((json == null) || json.isEmpty()) {
      sr.policy = RedactionPolicy.emptyRedactionPolicy();
      return sr;
    }

    ObjectMapper mapper = new ObjectMapper();
    RedactionPolicy policy = mapper.readValue(json, RedactionPolicy.class);
    policy.postProcess();
    sr.policy = policy;
    return sr;
  }

  public static StringRedactor createEmpty() {
    StringRedactor sr = new StringRedactor();
    sr.policy = RedactionPolicy.emptyRedactionPolicy();
    return sr;
  }

  /**
   * The actual redaction - given a message, look through the list of
   * redaction rules and apply if matching. If so, return the redacted
   * String, else return the original string.
   * @param msg The message to examine.
   * @return The (potentially) redacted message.
   */
  public String redact(String msg) {
    return policy.redact(msg);
  }
}
