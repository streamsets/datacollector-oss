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
package com.streamsets.pipeline.lib.util;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;


public class TestGlobFilePathUtil
{

  @Test
  public void testHasWildcard() {
    Assert.assertFalse(GlobFilePathUtil.hasGlobWildcard(""));
    Assert.assertFalse(GlobFilePathUtil.hasGlobWildcard("a"));
    Assert.assertFalse(GlobFilePathUtil.hasGlobWildcard("\\*"));
    Assert.assertFalse(GlobFilePathUtil.hasGlobWildcard("a\\?"));
    Assert.assertFalse(GlobFilePathUtil.hasGlobWildcard("\\[z"));
    Assert.assertFalse(GlobFilePathUtil.hasGlobWildcard("a\\{z"));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("*"));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("?"));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("["));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("{"));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("a*"));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("*z"));
    Assert.assertTrue(GlobFilePathUtil.hasGlobWildcard("a*z"));
  }

  @Test
  public void testPivotWildCardAndSubPath() {
    Path filePathWithBothWildCardAndPivot = Paths.get("/xx/*/data/file1.txt");
    Assert.assertEquals(
        GlobFilePathUtil.getPivotPath(filePathWithBothWildCardAndPivot),
        Paths.get("/xx")
    );
    Assert.assertEquals(
        GlobFilePathUtil.getWildcardPath(filePathWithBothWildCardAndPivot),
        Paths.get("*/data/file1.txt")
    );

    int wildcardIdx = 0;

    for (; wildcardIdx < filePathWithBothWildCardAndPivot.getNameCount() &&
        !GlobFilePathUtil.hasGlobWildcard(filePathWithBothWildCardAndPivot.getName(wildcardIdx).toString());
         wildcardIdx++);


    Assert.assertEquals(
        Paths.get("*/data/file1.txt"),
        GlobFilePathUtil.getSubPath(
            filePathWithBothWildCardAndPivot,
            wildcardIdx,
            filePathWithBothWildCardAndPivot.getNameCount(),
            true
        )
    );

    Assert.assertEquals(
        Paths.get("/xx"),
        GlobFilePathUtil.getSubPath(
            filePathWithBothWildCardAndPivot,
            0,
            wildcardIdx,
            true
        )
    );

    Path nonWildCardPath = Paths.get("/xx/data/file1.txt");

    Assert.assertEquals(
        GlobFilePathUtil.getPivotPath(nonWildCardPath),
        nonWildCardPath
    );

    Assert.assertEquals(
        GlobFilePathUtil.getWildcardPath(nonWildCardPath),
        null
    );

    Path rootPivotWildCardPath = Paths.get("/*/xx/file1.txt");
    Assert.assertEquals(
        GlobFilePathUtil.getPivotPath(rootPivotWildCardPath),
        Paths.get("/")
    );
    Assert.assertEquals(
        GlobFilePathUtil.getWildcardPath(rootPivotWildCardPath),
        Paths.get("*/xx/file1.txt")
    );

  }
}
