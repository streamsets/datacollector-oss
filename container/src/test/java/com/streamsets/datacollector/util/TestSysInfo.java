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
package com.streamsets.datacollector.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.SysInfo;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestSysInfo {

  private static final String AZURE_PROVIDER = "azure";
  private static final String AWS_METADATA_FIXTURE = "/com/streamsets/datacollector/util/awsInstanceMetadata.json";
  private static final String AZURE_METADATA_FIXTURE = "/com/streamsets/datacollector/util/azureInstanceMetadata.json";
  private static final String AWS_PROVIDER = "aws";
  private static final String CLOUD_PROVIDER = "cloudProvider";
  private static final String CLOUD_METADATA = "cloudMetadata";
  private static final String JAVA_VERSION = "javaVersion";

  private static RuntimeInfo runtimeInfo;
  private static SysInfo sysInfo;

  @Rule
  public Timeout globalTimeout= new Timeout(10, TimeUnit.SECONDS);

  @BeforeClass
  public static void beforeClass() {
    runtimeInfo = mock(RuntimeInfo.class);
    when(runtimeInfo.getLibexecDir()).thenReturn(
        System.getenv("PWD").replace("container/src/main/.*","") + "/dist/src/main/libexec");
    sysInfo = new SysInfo(runtimeInfo);
  }

  @AfterClass
  public static void afterClass() {
    runtimeInfo = null;
    sysInfo = null;
  }

  @Test
  public void testBasics() {
    assertNotNull(sysInfo.getCloudProvider());
    if (sysInfo.getCloudProvider().equals(SysInfo.UNKNOWN)) {
      assertNullOrEmpty(sysInfo.getCloudMetadata());
    } else {
      assertNotNullOrEmpty(sysInfo.getCloudMetadata());
    }
    assertNotNull(sysInfo.getDistributionChannel());
    assertNotNull(sysInfo.getJavaVersion());
    assertPositive(sysInfo.getJvmCores());
    assertPositive(sysInfo.getJvmMaxHeap());
    assertPositive(sysInfo.getJvmPeakMetaspaceUsed());
    assertPositive(sysInfo.getSysMem());
    assertPositive(sysInfo.getSysVCores());
    sysInfo.isUseCGroupMemorySet(); // make sure no exception
  }

  /**
   * To refresh this test's fixture, run:
   * <pre>
   *   scp "$SDC_GIT_ROOT"/dist/src/main/libexec/_sys_info_util "$USER"@"$AZURE_HOST":
   *   ssh "$USER"@"$AZURE_HOST" bash _sys_info_util get_cloud_metadata > "$SDC_GIT_ROOT"/container/src/test/resources/com/streamsets/datacollector/usagestats/azureInstanceMetadata.json
   * </pre>
   * You will need to update assertions for expected values as well.
   */
  @Test
  public void testAzureParseAndRedact() throws Exception {
    final String azureJson = IOUtils.toString(this.getClass().getResource(
        AZURE_METADATA_FIXTURE),
        Charsets.UTF_8
    );
    Map<String, Object> map = SysInfo.parseAndRedactMetadata(azureJson);
    assertTrue(map.containsKey("compute"));
    Map<String, Object> computeMap = getInnerMap(map, "compute");
    assertEquals(SysInfo.REDACTED_TEXT, computeMap.get("customData"));
    assertEquals(SysInfo.REDACTED_TEXT, computeMap.get("publicKeys"));
    assertEquals(SysInfo.REDACTED_TEXT, computeMap.get("tags"));
    assertEquals(SysInfo.REDACTED_TEXT, computeMap.get("tagsList"));
    assertNotNullOrEmpty((String) computeMap.get("subscriptionId"));

    // check nested maps and structures
    List<Object> interfaces = (List<Object>) getInnerMap(map, "network").get("interface");
    Map<String, Object> firstInterface = (Map<String, Object>) interfaces.get(0);
    Map<String, Object> ipv4 = getInnerMap(firstInterface, "ipv4");
    List<Object> ip4Addresses = (List<Object>) ipv4.get("ipAddress");
    assertEquals(
        ImmutableMap.of(
            "privateIpAddress", "172.11.12.13",
            "publicIpAddress", "138.11.12.13"),
        ip4Addresses.get(0));
  }

  /**
   * To refresh this test's fixture, run:
   * <pre>
   *   scp -i "$AWS_PEM_PATH" "$SDC_GIT_ROOT"/dist/src/main/libexec/_sys_info_util  ec2-user@"$AWS_HOST":
   *   ssh -i "$AWS_PEM_PATH" ec2-user@"$AWS_HOST" bash _sys_info_util get_cloud_metadata > "$SDC_GIT_ROOT"/container/src/test/resources/com/streamsets/datacollector/usagestats/awsInstanceMetadata.json
   * </pre>
   */
  @Test
  public void testAwsParse() throws Exception {
    // Though we will redact in SysInfo, the contents are already redacted by the shell script for aws
    final String awsJson = IOUtils.toString(this.getClass().getResource(
        AWS_METADATA_FIXTURE),
        Charsets.UTF_8
    );
    Map<String, Object> map = SysInfo.parseAndRedactMetadata(awsJson);
    assertNotNullOrEmpty((String) map.get("instance-type"));
  }

  @Test
  public void testHeuristicParseAndRedact() throws Exception {
    Map<String, Object> origMap = ImmutableMap.of(
        "noRedact", "keepMe1",
        "redactThisKey", "secretVal1",
        "childMapNeedsRedaction", ImmutableMap.of(
            "passwordToRedact", "secretVal2",
            "noRedactChild", "keepMe2"
        ),
        "childListNeedsRedaction", ImmutableList.of(
            "normalListElement",
            ImmutableMap.of(
                "listMapNoRedact", "keepMe3",
                "mysecretChildListToRedact", ImmutableList.of(
                    "secretVal3"
                ),
                "listMapCredentialRedact", "secretVal4"
            )
        )
    );
    Map<String, Object> expectedMap = ImmutableMap.of(
        "noRedact", "keepMe1",
        "redactThisKey", SysInfo.REDACTED_TEXT,
        "childMapNeedsRedaction", ImmutableMap.of(
            "passwordToRedact", SysInfo.REDACTED_TEXT,
            "noRedactChild", "keepMe2"
        ),
        "childListNeedsRedaction", ImmutableList.of(
            "normalListElement",
            ImmutableMap.of(
                "listMapNoRedact", "keepMe3",
                "mysecretChildListToRedact", SysInfo.REDACTED_TEXT,
                "listMapCredentialRedact", SysInfo.REDACTED_TEXT
            )
        )
    );

    String json = ObjectMapperFactory.get().writeValueAsString(origMap);
    assertTrue(json.contains("secretVal1"));
    assertTrue(json.contains("secretVal2"));
    assertTrue(json.contains("secretVal3"));
    assertTrue(json.contains("secretVal4"));
    Map<String, Object> redacted = SysInfo.parseAndRedactMetadata(json);
    assertEquals(expectedMap, redacted);
  }

  private Map<String, Object> getInnerMap(Map<String, Object> map, String ... path) {
    for (int i = 0; i < path.length; i++) {
      String nextKey = path[i];
      assertNotNull(nextKey);
      Object innerObject = map.get(nextKey);
      assertNotNull("Could not find element " + i + " in path " + Arrays.toString(path), innerObject);
      assertTrue("Expected map type but got: " + innerObject, innerObject instanceof Map);
      map = (Map<String, Object>) innerObject;
    }
    return map;
  }

  @Test
  public void testToMap() {
    Map<String, Object> map = sysInfo.toMap();
    assertEquals(sysInfo.getJavaVersion(), map.get("javaVersion"));
    assertEquals(sysInfo.isUseCGroupMemorySet(), map.get("useCGroupMemorySet"));
    // algorithm is generic, so just need to spot check
  }

  @Test
  public void testEmptyCloudMetadataToMap() {
    SysInfo localSysInfo = new SysInfo(runtimeInfo) {
      @Override
      protected <T> Future<T> createScriptFuture(String name, String script, Map<String, String> additionalEnv, StdOutConverter<T> converter) {
        switch (name) {
          case SysInfo.CLOUD_PROVIDER_SCRIPT_ID:
            return (Future<T>) hardcodedFuture(SysInfo.UNKNOWN);
          case SysInfo.CLOUD_METADATA_SCRIPT_ID:
            return (Future<T>) hardcodedFuture(ImmutableMap.<String, Object>of());
          default:
            return (Future<T>) hardcodedFuture(null);
        }
      }
    };
    Map<String, Object> map = localSysInfo.toMap();
    assertEquals(SysInfo.UNKNOWN, map.get(CLOUD_PROVIDER));
    Map<String, Object> cloudMetadata = getInnerMap(map, CLOUD_METADATA);
    assertNotNull(cloudMetadata);
    assertEquals(0, cloudMetadata.size());

    assertNotNullOrEmpty((String) map.get(JAVA_VERSION));
  }

  @Test
  public void testAzureToMap() throws Exception {
    final String azureJson = IOUtils.toString(this.getClass().getResource(
        AZURE_METADATA_FIXTURE),
        Charsets.UTF_8
    );
    SysInfo localSysInfo = new SysInfo(runtimeInfo) {
      @Override
      protected <T> Future<T> createScriptFuture(String name, String script, Map<String, String> additionalEnv, StdOutConverter<T> converter) {
        switch (name) {
          case SysInfo.CLOUD_PROVIDER_SCRIPT_ID:
            return (Future<T>) hardcodedFuture(AZURE_PROVIDER);
          case SysInfo.CLOUD_METADATA_SCRIPT_ID:
            return (Future<T>) hardcodedFuture(parseAndRedactMetadata(azureJson));
          default:
            return (Future<T>) hardcodedFuture(null);
        }
      }
    };

    Map<String, Object> map = localSysInfo.toMap();
    assertEquals(AZURE_PROVIDER, map.get("cloudProvider"));
    Map<String, Object> cloudMetadata = getInnerMap(map, "cloudMetadata");
    assertNotNullOrEmpty(cloudMetadata);
    assertEquals("AzurePublicCloud", getInnerMap(map, "cloudMetadata", "compute").get("azEnvironment"));

    assertNotNullOrEmpty((String) map.get("javaVersion"));
  }

  @Test
  public void testAwsToMap() throws Exception {
    final String awsJson = IOUtils.toString(this.getClass().getResource(
        AWS_METADATA_FIXTURE),
        Charsets.UTF_8
    );
    SysInfo localSysInfo = new SysInfo(runtimeInfo) {
      @Override
      protected <T> Future<T> createScriptFuture(String name, String script, Map<String, String> additionalEnv, StdOutConverter<T> converter) {
        switch (name) {
          case SysInfo.CLOUD_PROVIDER_SCRIPT_ID:
            return (Future<T>) hardcodedFuture(AWS_PROVIDER);
          case SysInfo.CLOUD_METADATA_SCRIPT_ID:
            return (Future<T>) hardcodedFuture(parseAndRedactMetadata(awsJson));
          default:
            return (Future<T>) hardcodedFuture(null);
        }
      }
    };

    Map<String, Object> map = localSysInfo.toMap();
    assertEquals(AWS_PROVIDER, map.get("cloudProvider"));
    Map<String, Object> cloudMetadata = getInnerMap(map, "cloudMetadata");
    assertNotNullOrEmpty(cloudMetadata);
    assertEquals("amazonaws.com", getInnerMap(map, "cloudMetadata", "services").get("domain"));

    assertNotNullOrEmpty((String) map.get("javaVersion"));
  }

  @Test
  public void testToMapExceptionHandling() {
    SysInfo localSysInfo = new SysInfo(runtimeInfo) {
      @Override
      protected <T> Future<T> createScriptFuture(String name, String script, Map<String, String> additionalEnv, StdOutConverter<T> converter) {
        switch (name) {
          case SysInfo.CLOUD_METADATA_SCRIPT_ID:
            return new Future<T>() {
              @Override
              public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
              }

              @Override
              public boolean isCancelled() {
                return false;
              }

              @Override
              public boolean isDone() {
                return true;
              }

              @Override
              public T get() throws InterruptedException, ExecutionException {
                throw new RuntimeException("Code should swallow this exception");
              }

              @Override
              public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                throw new RuntimeException("Code should swallow this exception");
              }
            };
          default:
            return (Future<T>) hardcodedFuture(null);
        }
      }
    };
    Map<String, Object> map = localSysInfo.toMap();
    assertNull(map);
  }

  @Test
  public void testScriptFuture() throws Exception {
    SysInfo.ScriptProcessFuture<String> future = (SysInfo.ScriptProcessFuture<String>) sysInfo.createScriptFuture(
        "testEchoYes",
        "echo Yes",
        null,
        (String stdout) -> stdout);
    future = spy(future);
    assertEquals("Yes", future.get(10, TimeUnit.SECONDS));
    verify(future, never()).getCutoffTime(); // we should not hit any timeout, nor check cutoff time
    verify(future, never()).cancel(anyBoolean());

    // make sure we use cached result for future calls
    when(future.getP()).then(invocation -> {
      fail("Should hit cache instead of checking process");
      return null;
    });
    assertEquals("Yes", future.get());
  }

  @Test
  public void testScriptFutureTimeout() throws Exception {
    SysInfo.ScriptProcessFuture<String> future = (SysInfo.ScriptProcessFuture<String>) sysInfo.createScriptFuture(
        "testEchoYes",
        "echo mocked below",
        null,
        (String stdout) -> stdout);

    future = spy(future);
    Process p = mock(Process.class);
    when(p.waitFor(1, TimeUnit.MILLISECONDS)).thenReturn(false);
    when(future.getP()).thenReturn(p);

    String result = future.get(1, TimeUnit.MILLISECONDS);
    assertNull("Should have returned null due to timeout", result);
    verify(p).waitFor(1, TimeUnit.MILLISECONDS);
    verify(p, never()).destroyForcibly();
    verify(p, never()).exitValue();

    final String expected = "expectedResult";
    // now let the process complete and make sure we didn't cache the result last time
    when(p.waitFor(1, TimeUnit.MILLISECONDS)).thenReturn(true);
    when(p.exitValue()).thenReturn(0);
    when(p.getInputStream()).thenReturn(IOUtils.toInputStream(expected));

    result = future.get(1, TimeUnit.MILLISECONDS);
    verify(p, times(2)).waitFor(1, TimeUnit.MILLISECONDS);
    verify(p, never()).destroyForcibly();
    verify(p).exitValue();
    verify(p).getInputStream();

    assertEquals(expected, result);

    // make sure we use cached result for future calls
    when(future.getP()).then(invocation -> {
      fail("Should hit cache instead of checking process");
      return null;
    });
    assertEquals(expected, future.get());
  }

  @Test
  public void testScriptFutureTimeoutExceedsCutoff() throws Exception {
    SysInfo.ScriptProcessFuture<String> future = (SysInfo.ScriptProcessFuture<String>) sysInfo.createScriptFuture(
        "testEchoYes",
        "echo mocked below",
        null,
        (String stdout) -> stdout);

    future = spy(future);
    Process p = mock(Process.class);
    when(p.waitFor(1, TimeUnit.MILLISECONDS)).thenReturn(false);
    when(future.getP()).thenReturn(p);

    String result = future.get(1, TimeUnit.MILLISECONDS);
    assertNull("Should have returned null due to timeout", result);
    verify(p).waitFor(1, TimeUnit.MILLISECONDS);
    verify(p, never()).destroyForcibly();
    verify(p, never()).exitValue();

    // now trigger cutoff
    when(future.getCutoffTime()).thenReturn(System.currentTimeMillis() - 1);
    when(p.isAlive()).thenReturn(true);
    assertNull("Should have returned null and cached it", future.get());
    verify(p).destroyForcibly();
    verify(p, never()).exitValue();

    // make sure we use cached result for future calls
    when(future.getP()).then(invocation -> {
      fail("Should hit cache instead of checking process");
      return null;
    });
    assertNull(future.get());
  }

  private void assertNullOrEmpty(String value) {
    assertTrue("Expected null or empty, but was: <" + value + ">",
        value == null || value.isEmpty());
  }

  private <K, V> void assertNullOrEmpty(Map<K, V> value) {
    assertTrue("Expected null or empty, but was: <" + value + ">",
        value == null || value.isEmpty());
  }

  private void assertNotNullOrEmpty(String value) {
    assertTrue("Expected non-null non-empty string, but was: <" + value + ">",
        value != null && !value.isEmpty());
  }

  private <K, V> void assertNotNullOrEmpty(Map<K, V> value) {
    assertTrue("Expected non-null non-empty string, but was: <" + value + ">",
        value != null && !value.isEmpty());
  }

  private void assertPositive(long value) {
    assertTrue("Expected positive number, but was <" + value + ">",
        value > 0L);
  }
}
