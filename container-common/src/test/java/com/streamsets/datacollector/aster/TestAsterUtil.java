/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.aster;


import com.streamsets.datacollector.main.RuntimeInfo;
import jersey.repackaged.com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestAsterUtil {

  @Test
  public void testAsterJars() throws Exception {
    File asterClientFolder = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(asterClientFolder.mkdirs());

    IOUtils.write("abc", new FileWriter(new File(asterClientFolder, "a.jar")));
    IOUtils.write("abc", new FileWriter(new File(asterClientFolder, "b.jar")));
    IOUtils.write("abc", new FileWriter(new File(asterClientFolder, "c.jar")));
    IOUtils.write("abc", new FileWriter(new File(asterClientFolder, "d.txt")));

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.doReturn(asterClientFolder.getAbsolutePath()).when(runtimeInfo).getAsterClientDir();

    List<URL> urls = AsterUtil.getAsterJars(runtimeInfo);
    Assert.assertEquals(3, urls.size());

    Set<String> expectedFiles = Sets.newHashSet("a.jar", "b.jar", "c.jar");

    Set<String> actualFiles = urls.stream().map(u -> new File(u.getPath()).getName()).collect(Collectors.toSet());
    Assert.assertEquals(expectedFiles, actualFiles);
  }
}
