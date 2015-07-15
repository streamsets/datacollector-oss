package com.streamsets.pipeline.lib.fuzzy;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class FuzzyMatch {
  private static final Logger LOG = LoggerFactory.getLogger(FuzzyMatch.class);

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

    s1 = escapeString(s1);
    s2 = escapeString(s2);

    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();


    Set<String> set1 = new HashSet<>();
    Set<String> set2 = new HashSet<>();

    //split the string by space and store words in sets
    StringTokenizer st1 = new StringTokenizer(s1);
    while (st1.hasMoreTokens()) {
      set1.add(st1.nextToken());
    }

    StringTokenizer st2 = new StringTokenizer(s2);
    while (st2.hasMoreTokens()) {
      set2.add(st2.nextToken());
    }

    SetView<String> intersection = Sets.intersection(set1, set2);

    TreeSet<String> sortedIntersection = Sets.newTreeSet(intersection);

    if (LOG.isDebugEnabled()) {
      StringBuilder sortedSb = new StringBuilder();
      for (String s : sortedIntersection) {
        sortedSb.append(s).append(" ");
      }
      LOG.debug("Sorted intersection --> {}", sortedSb.toString());
    }

    // Find out difference of sets set1 and intersection of set1,set2
    SetView<String> restOfSet1 = Sets.symmetricDifference(set1, intersection);

    // Sort it
    TreeSet<String> sortedRestOfSet1 = Sets.newTreeSet(restOfSet1);

    SetView<String> restOfSet2 = Sets.symmetricDifference(set2, intersection);
    TreeSet<String> sortedRestOfSet2 = Sets.newTreeSet(restOfSet2);

    if (LOG.isDebugEnabled()) {
      StringBuilder sb1 = new StringBuilder();
      for (String s : sortedRestOfSet1) {
        sb1.append(s).append(" ");
      }
      LOG.debug("Sorted rest of 1 --> {}", sb1.toString());

      StringBuilder sb2 = new StringBuilder();
      for (String s : sortedRestOfSet1) {
        sb2.append(s).append(" ");
      }
      LOG.debug("Sorted rest of 2 --> {}", sb2.toString());
    }

    String t0 = "";
    String t1 = "";
    String t2 = "";

    for (String s : sortedIntersection) {
      t0 = t0 + " " + s;
    }
    t0 = t0.trim();

    Set<String> setT1 = Sets.union(sortedIntersection, sortedRestOfSet1);
    for (String s : setT1) {
      t1 = t1 + " " + s;
    }
    t1 = t1.trim();

    Set<String> setT2 = Sets.union(intersection, sortedRestOfSet2);
    for (String s : setT2) {
      t2 = t2 + " " + s;
    }

    t2 = t2.trim();

    int amt1 = calculateLevenshteinDistance(t0, t1);
    int amt2 = calculateLevenshteinDistance(t0, t2);
    int amt3 = calculateLevenshteinDistance(t1, t2);

    LOG.debug("t0 = {} --> {}", t0, amt1);
    LOG.debug("t1 = {} --> {}", t1, amt2);
    LOG.debug("t2 = {} --> {}", t2, amt3);

    return Math.max(Math.max(amt1, amt2), amt3);
  }

  public static int calculateLevenshteinDistance(String s1, String s2) {
    int distance = StringUtils.getLevenshteinDistance(s1, s2);
    double ratio = ((double) distance) / (Math.max(s1.length(), s2.length()));
    return 100 - new Double(ratio * 100).intValue();
  }

  public static String escapeString(String token) {
    final Pattern p = Pattern.compile("[^\\w+]", Pattern.UNICODE_CHARACTER_CLASS);
    return p.matcher(token).replaceAll(" ");
  }
}
