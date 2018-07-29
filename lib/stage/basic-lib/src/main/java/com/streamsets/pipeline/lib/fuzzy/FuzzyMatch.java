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
package com.streamsets.pipeline.lib.fuzzy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class FuzzyMatch {
  private static final Logger LOG = LoggerFactory.getLogger(FuzzyMatch.class);
  private static final Pattern removeNonAlphaNum = Pattern.compile("[^\\w+]", Pattern.UNICODE_CHARACTER_CLASS);
  private static final Pattern camelAndSnakeCaseSplitter = Pattern.compile("(?=\\p{Lu})|(_)");

  private FuzzyMatch() {}

  // Build a cache with sane defaults. Shouldn't have to ask user to configure this.
  private static LoadingCache<Pair<String, String>, Integer> ratioCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(1, TimeUnit.SECONDS)
      .build(
          new CacheLoader<Pair<String, String>, Integer>() {
            @Override
            public Integer load(Pair<String, String> pair) {
              return FuzzyMatch.getRatio(pair.getLeft(), pair.getRight());
            }
          }
      );

  public static int getRatio(String s1, String s2, boolean useCache) {
    if (useCache) {
     return ratioCache.getUnchecked(Pair.of(s1, s2));
    } else {
      return getRatio(s1, s2);
    }
  }

  /*
   * t0 = [SORTED_INTERSECTION]
   * t1 = [SORTED_INTERSECTION] + [SORTED_REST_OF_STRING1]
   * t2 = [SORTED_INTERSECTION] + [SORTED_REST_OF_STRING2]
   *
   * outcome = max(t0,t1,t2)
   *
   */

  public static int getRatio(String s1, String s2) {

    if (s1.length() >= s2.length()) {
      // We need to swap s1 and s2
      String temp = s2;
      s2 = s1;
      s1 = temp;
    }

    // Get alpha numeric characters
    Set<String> set1 = tokenizeString(escapeString(s1));
    Set<String> set2 = tokenizeString(escapeString(s2));

    SetView<String> intersection = Sets.intersection(set1, set2);

    TreeSet<String> sortedIntersection = Sets.newTreeSet(intersection);

    if (LOG.isTraceEnabled()) {
      StringBuilder sortedSb = new StringBuilder();
      for (String s : sortedIntersection) {
        sortedSb.append(s).append(" ");
      }
      LOG.trace("Sorted intersection --> {}", sortedSb.toString());
    }

    // Find out difference of sets set1 and intersection of set1,set2
    SetView<String> restOfSet1 = Sets.symmetricDifference(set1, intersection);

    // Sort it
    TreeSet<String> sortedRestOfSet1 = Sets.newTreeSet(restOfSet1);

    SetView<String> restOfSet2 = Sets.symmetricDifference(set2, intersection);
    TreeSet<String> sortedRestOfSet2 = Sets.newTreeSet(restOfSet2);

    if (LOG.isTraceEnabled()) {
      StringBuilder sb1 = new StringBuilder();
      for (String s : sortedRestOfSet1) {
        sb1.append(s).append(" ");
      }
      LOG.trace("Sorted rest of 1 --> {}", sb1.toString());

      StringBuilder sb2 = new StringBuilder();
      for (String s : sortedRestOfSet1) {
        sb2.append(s).append(" ");
      }
      LOG.trace("Sorted rest of 2 --> {}", sb2.toString());
    }

    StringBuilder t0Builder = new StringBuilder("");
    StringBuilder t1Builder = new StringBuilder("");
    StringBuilder t2Builder = new StringBuilder("");

    for (String s : sortedIntersection) {
      t0Builder.append(" ").append(s);
    }
    String t0 = t0Builder.toString().trim();

    Set<String> setT1 = Sets.union(sortedIntersection, sortedRestOfSet1);
    for (String s : setT1) {
      t1Builder.append(" ").append(s);
    }
    String t1 = t1Builder.toString().trim();

    Set<String> setT2 = Sets.union(intersection, sortedRestOfSet2);
    for (String s : setT2) {
      t2Builder.append(" ").append(s);
    }

    String t2 = t2Builder.toString().trim();

    int amt1 = calculateLevenshteinDistance(t0, t1);
    int amt2 = calculateLevenshteinDistance(t0, t2);
    int amt3 = calculateLevenshteinDistance(t1, t2);

    LOG.trace("t0 = {} --> {}", t0, amt1);
    LOG.trace("t1 = {} --> {}", t1, amt2);
    LOG.trace("t2 = {} --> {}", t2, amt3);

    return Math.max(Math.max(amt1, amt2), amt3);
  }

  private static Set<String> tokenizeString(String str) {
    Set<String> set = new HashSet<>();

    // Normalize and tokenize the input strings before storing as a set.
    StringTokenizer st = new StringTokenizer(str);
    while (st.hasMoreTokens()) {
      String t1 = st.nextToken();
      String[] tokens = camelAndSnakeCaseSplitter.split(t1);
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].toLowerCase();
      }
      Collections.addAll(set, tokens);
    }

    set.remove("");

    return set;
  }

  private static int calculateLevenshteinDistance(String s1, String s2) {
    int distance = StringUtils.getLevenshteinDistance(s1, s2);
    double ratio = ((double) distance) / (Math.max(s1.length(), s2.length()));
    return 100 - (int)(ratio * 100);
  }

  private static String escapeString(String token) {
    return removeNonAlphaNum.matcher(token).replaceAll(" ");
  }
}
