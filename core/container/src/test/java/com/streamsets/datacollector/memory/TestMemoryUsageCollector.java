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
package com.streamsets.datacollector.memory;

import com.streamsets.datacollector.memory.MemoryUsageCollector;
import com.streamsets.pipeline.api.impl.Utils;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.Random;

public class TestMemoryUsageCollector {
  private static final Logger LOG = LoggerFactory.getLogger(TestMemoryUsageCollector.class);
  public static void initalizeMemoryUtility() throws Exception {
    Class clazz = Class.forName("com.streamsets.pipeline.BootstrapMain");
    Field field = clazz.getDeclaredField("instrumentation");
    field.setAccessible(true);
    Instrumentation instrumentation = (Instrumentation) field.get(null);
    if (instrumentation == null) {
      throw new IllegalStateException("Field BootstrapMain.instrumentation is null");
    }
    MemoryUsageCollector.initialize(instrumentation);
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    initalizeMemoryUtility();
  }

  @Test
  public void testBasic() throws Exception {
    Assert.assertEquals(16, MemoryUsageCollector.getMemoryUsageOfForTests(new Object()));
    Assert.assertEquals(16, MemoryUsageCollector.getMemoryUsageOfForTests(1));
    Assert.assertEquals(0, MemoryUsageCollector.getMemoryUsageOfForTests(null));
    Assert.assertEquals(56, MemoryUsageCollector.getMemoryUsageOfForTests(new int[10]));
    Assert.assertEquals(1040, MemoryUsageCollector.getMemoryUsageOfForTests(new byte[1024]));
    Assert.assertEquals(16, MemoryUsageCollector.getMemoryUsageOfForTests(new ClassWithNoFields()));
    Assert.assertEquals(16, MemoryUsageCollector.getMemoryUsageOfForTests(new ClassWithOnePrimitiveField()));
    Assert.assertEquals(16, MemoryUsageCollector.getMemoryUsageOfForTests(new ClassWithNoAdditionalFields()));
    Assert.assertEquals(64, MemoryUsageCollector.getMemoryUsageOfForTests(new ClassWithOneStringField()));
    Assert.assertEquals(32, MemoryUsageCollector.getMemoryUsageOfForTests(new ClassWithOneComplexField()));
  }
  @Test
  public void testClassWithStaticRefToObjectWithNonStaticRef() throws Exception {
    long value = MemoryUsageCollector.
      getMemoryUsageOfForTests(ClassWithStaticRefToObjectWithNonStaticRef.class);
    // range is due to JVM differences
    Assert.assertTrue("Result " + value + " is not correct", value > 500 && value < 600);
  }
  @Test
  public void testClassWithStaticData() throws Exception {
    // for tests we cannot include the classloader since it will vary considerably
    // 1024 + 16 for object reference + 16 for static data reference = 1056
    Assert.assertEquals(1056, MemoryUsageCollector.getMemoryUsageOfForTests(new ClassWithStaticData()));
    long value = MemoryUsageCollector.
      getMemoryUsageOfForTests(ClassWithStaticRefToObjectWithStaticRef.class);
    Assert.assertTrue("Result " + value + " is not correct", value > 1500 && value < 1650);
  }
  @Test
  public void testClassWithStaticRefToObjectWithStaticRef() throws Exception {
    long value = MemoryUsageCollector.
      getMemoryUsageOfForTests(ClassWithStaticRefToObjectWithStaticRef.class);
    Assert.assertTrue("Result " + value + " is not correct", value > 1500 && value < 1650);
  }
  @Test
  public void testAnonymousObjectIntMember() throws Exception {
    Assert.assertEquals(24, MemoryUsageCollector.getMemoryUsageOfForTests(new Object() {
      private int x = 1;
    }));
  }
  @Test
  public void testRandom() throws Exception {
    class Node {
      Node node;
    }
    Random random = new Random(System.currentTimeMillis());
    LinkedList<Node> nodes = new LinkedList<Node>();
    nodes.add(new Node());
    for (int i = 0; i < 500; i++) {
      Node node = new Node();
      nodes.add(node);
      node.node = nodes.get(random.nextInt(nodes.size()));
    }
    Assert.assertEquals(24080, MemoryUsageCollector.getMemoryUsageOfForTests(nodes));
  }
  @Test
  public void testFourAnonymousLevels() throws Exception {
    class Recursive {
      Object o1;
      Object o2;
    }
    Recursive r1 = new Recursive();
    Recursive r2 = new Recursive();
    Recursive r3 = new Recursive();
    Recursive r4 = new Recursive();
    r1.o1 = r2;
    r2.o1 = r3;
    r3.o1 = r4;
    r4.o1 = r1;
    r1.o2 = r1;
    r2.o2 = r3;
    r3.o2 = r1;
    r4.o2 = r4;
    Assert.assertEquals(96, MemoryUsageCollector.getMemoryUsageOfForTests(r1));
    Assert.assertEquals(96, MemoryUsageCollector.getMemoryUsageOfForTests(r1));
  }
  @Test
  public void testAnonymousObjects() throws Exception {
    Assert.assertEquals(40, MemoryUsageCollector.getMemoryUsageOfForTests(new Object() {
      private Object field = new Object();
    }));
    Assert.assertEquals(56, MemoryUsageCollector.getMemoryUsageOfForTests(new Object() {
      private int x = 1;
      private int y = 2;
      private int z = 3;
      private Object field1 = new Object();
      private Object field2;
    }));
  }
  @Test
  public void testCustomClassLoader() throws Exception {
    // test traversal of static fields in classes loaded by custom class loaders
    int byteArraySize = 1024 * 1024 * 10;
    ClassLoader classLoader = new URLClassLoader(new URL[0], ClassLoader.getSystemClassLoader());
    ClassPool cp = new ClassPool();
    cp.appendSystemPath();
    CtClass testClass = cp.makeClass("TestClass" + System.currentTimeMillis());
    CtClass byteArrayClass = cp.get(Byte.TYPE.getName() + "[]");
    CtField testField = CtField.make("public static byte[] buffer;", testClass);
    testClass.addField(testField, CtField.Initializer.byNewArray(byteArrayClass, byteArraySize));
    cp.toClass(testClass, classLoader, null);
    long size = MemoryUsageCollector.getMemoryUsageOfForTests(classLoader, classLoader, true);
    Assert.assertTrue("Expected " + Utils.humanReadableInt(size) + " to be greater than 10MB",
      size > byteArraySize);
    Assert.assertTrue("Expected " + Utils.humanReadableInt(size) + " to be less than 20MB",
      size < 2*byteArraySize);
  }
  private static class ClassWithNoFields {

  }
  private static class ClassWithOnePrimitiveField {
    private int x = 1;
  }
  private static class ClassWithNoAdditionalFields extends ClassWithOnePrimitiveField {

  }
  private static class ClassWithOneStringField {
    private String s = "ABC";
  }
  private static class ClassWithOneComplexField {
    private Object field = new Object();
  }
  private static class ClassWithStaticData {
    private static final byte[] bytes = new byte[1024];
  }
  private static class ClassWithStaticRefToObjectWithNonStaticRef {
    private static final ClassWithOneComplexField field = new ClassWithOneComplexField();
  }
  private static class ClassWithStaticRefToObjectWithStaticRef {
    private static final ClassWithStaticData field = new ClassWithStaticData();
  }
}
