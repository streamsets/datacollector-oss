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

import com.carrotsearch.hppc.IntHashSet;
import com.streamsets.datacollector.runner.StageRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.SubjectDomainCombiner;
import java.lang.instrument.Instrumentation;
import java.lang.ref.PhantomReference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Deque;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Collects memory on a given object and classloader. For example in the context of StreamSets
 * we'd like to ensure a given stage and classloader do not exceed a given amount of memory.
 */
public class MemoryUsageCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryUsageCollector.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static final Field[] EMPTY_FIELD_ARRAY = new Field[0];
  private static final Field CLASSLOADER_CLASSES_FIELD;
  private static Instrumentation sharedInstrumentation;
  private static final ConcurrentMap<Class, Field[]> classToFieldCache = new ConcurrentHashMap<>();
  private final Instrumentation instrumentation;
  private final Object targetObject;
  private final ClassLoader targetClassloader;
  private final Deque<Object> stack;
  private final IntHashSet countedObjectSet;
  private final boolean traverseClassLoaderClasses;
  private final MemoryUsageSnapshot memoryUsageSnapshot;
  private Collection<Class<?>> classes;

  static {
    // initialize CLASSLOADER_CLASSES_FIELD
    Field classLoaderClasses = null;
    try {
      classLoaderClasses = ClassLoader.class.getDeclaredField("classes");
      classLoaderClasses.setAccessible(true);
      Object classes = classLoaderClasses.get(ClassLoader.getSystemClassLoader());
      if (classes == null) {
        throw new IllegalArgumentException("Classes field is null");
      }
      if (!(classes instanceof Vector)) {
        throw new IllegalArgumentException("Classes field is not a vector: " + classes.getClass());
      }
    } catch(Exception e) {
      String msg = "Could not find field ClassLoader.classes. Heap will not be monitored. Error: " + e;
      LOG.error(msg, e);
    }
    CLASSLOADER_CLASSES_FIELD = classLoaderClasses;

    Field subjectDomainInternalLock = null;
    try {
      subjectDomainInternalLock = SubjectDomainCombiner.class.getDeclaredField("cachedPDs");
      subjectDomainInternalLock.setAccessible(true);
    } catch (Exception e) {
      String msg = "Could not obtain internal SubjectDomainCombiner lock. Error: " + e;
      LOG.error(msg, e);
    }
  }

  public synchronized static void initialize(Instrumentation sharedInstrumentation) {
    MemoryUsageCollector.sharedInstrumentation = sharedInstrumentation;
  }

  public static class Builder {
    private StageRuntime stageRuntime;
    private boolean traverseClassLoaderClasses = true;
    private MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle;

    public Builder setTraverseClassLoaderClasses(boolean traverseClassLoaderClasses) {
      this.traverseClassLoaderClasses = traverseClassLoaderClasses;
      return this;
    }
    public Builder setStageRuntime(StageRuntime stageRuntime) {
      this.stageRuntime = stageRuntime;
      return this;
    }
    public Builder setMemoryUsageCollectorResourceBundle(
      MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle) {
      this.memoryUsageCollectorResourceBundle = memoryUsageCollectorResourceBundle;
      return this;
    }
    public MemoryUsageCollector build() {
      if (sharedInstrumentation == null) {
        throw new IllegalStateException("MemoryUsageCollector has not been initialized");
      }
      if (memoryUsageCollectorResourceBundle == null) {
        memoryUsageCollectorResourceBundle = new MemoryUsageCollectorResourceBundle();
      }
      MemoryUsageCollector result = new MemoryUsageCollector(sharedInstrumentation, stageRuntime.getStage(),
        stageRuntime.getDefinition().getStageClassLoader(), memoryUsageCollectorResourceBundle.getStack(),
        memoryUsageCollectorResourceBundle.getObjectSet(), traverseClassLoaderClasses);
      result.initialize();
      return result;
    }
  }

  @SuppressWarnings("unchecked")
  private MemoryUsageCollector(Instrumentation instrumentation, Object targetObject, ClassLoader targetClassloader,
                               Deque stack, IntHashSet countedObjectSet,
                               boolean traverseClassLoaderClasses) {
    this.instrumentation = instrumentation;
    this.targetObject = targetObject;
    this.targetClassloader = targetClassloader;
    this.traverseClassLoaderClasses = traverseClassLoaderClasses;
    this.stack = stack;
    this.countedObjectSet = countedObjectSet;
    this.memoryUsageSnapshot = new MemoryUsageSnapshot(targetObject, targetClassloader);
  }

  @SuppressWarnings("unchecked")
  private void initialize() {
    try {
      if (CLASSLOADER_CLASSES_FIELD == null) {
        if (traverseClassLoaderClasses) {
          LOG.debug("Cannot traverse classloader classes since classes field is not valid");
        }
      } else {
        Vector<Class<?>> classes = (Vector<Class<?>>) CLASSLOADER_CLASSES_FIELD.get(targetClassloader);
        if (classes != null) {
          // note that we cannot synchronize on this vector because in the process of traversing
          // the classes inside we may load additional classes which will result in the classloader
          // trying to obtain a lock on the classloader. However there could be another thread
          // which holds the lock on the classloader and is waiting for a lock on this vector
          // in any case we should not need a lock since we are doing read-only traversal of the
          // the members of the vector
          memoryUsageSnapshot.setNumClassesLoaded(classes.size());
          this.classes = classes;
        }
      }
    } catch (Exception e) {
      String msg = "Error getting classes from classLoader: " + e;
      throw new IllegalStateException(msg, e);
    }
  }

  public MemoryUsageSnapshot collect() {
    stack.clear();
    countedObjectSet.release();
    long startInstances = System.currentTimeMillis();
    long memoryConsumedByInstances = getMemoryUsageIterative(targetObject, targetClassloader);
    memoryUsageSnapshot.addMemoryConsumedByInstances(memoryConsumedByInstances);
    memoryUsageSnapshot.addElapsedTimeByInstances(System.currentTimeMillis() - startInstances);
    if (traverseClassLoaderClasses) {
      long startClasses = System.currentTimeMillis();
      if (classes != null) {
        long memoryConsumedByClasses = getMemoryUsageIterative(classes, targetClassloader);
        memoryUsageSnapshot.addMemoryConsumedByClasses(memoryConsumedByClasses);
      }
      memoryUsageSnapshot.addElapsedTimeByClasses(System.currentTimeMillis() - startClasses);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("MemoryUsageSnapshot = " + memoryUsageSnapshot);
    }
    return memoryUsageSnapshot;
  }
  /**
   * Visible for tests only
   */
  public static long getMemoryUsageOfForTests(Object obj)
    throws Exception {
    return getMemoryUsageOfForTests(obj, ClassLoader.getSystemClassLoader(), false);
  }
  /**
   * Visible for tests only
   */
  public static long getMemoryUsageOfForTests(Object obj, ClassLoader classLoader, boolean traverseClassLoaderClasses)
  throws Exception {
    if (sharedInstrumentation == null) {
      throw new IllegalStateException("MemoryUtility has not been initialized");
    }
    MemoryUsageCollectorResourceBundle bundle = new MemoryUsageCollectorResourceBundle();
    MemoryUsageCollector collector = new MemoryUsageCollector(sharedInstrumentation, obj, classLoader,
      bundle.getStack(), bundle.getObjectSet(), traverseClassLoaderClasses);
    collector.initialize();
    MemoryUsageSnapshot snapshot = collector.collect();
    LOG.info("MemoryUsageSnapshot = " + snapshot);
    return snapshot.getMemoryConsumed();
  }
  /**
   * Finds the memory consumption of an object.
   *
   * Note for debugging, if you need to call obj.toString() you need to wrap that in try-catch
   * as some objects we will find will throw a NPE when toString() is called.
   */

  private long getMemoryUsageIterative(Object obj, ClassLoader classLoader) {
    long total = 0L;
    if (obj != null) {
      stack.push(obj);
    }
    while (!stack.isEmpty()) {
      obj = stack.pop();
      if (obj == null || obj instanceof PhantomReference || obj instanceof WeakReference
        || obj instanceof SoftReference) {
        continue;
      }
      boolean isObjectClass = (obj instanceof Class);
      Class clz;
      if (isObjectClass) {
        // if object is a class, we want to inspect the class represented as the object
        // not the classes class which is java.lang.Class
        clz = (Class)obj;
      } else {
        clz = obj.getClass();
      }
      boolean isClassOwnedByClassLoader = clz.getClassLoader() == classLoader;
      int objectId = System.identityHashCode(obj);
      if (countedObjectSet.add(objectId)) {
        long objSizeInBytes = instrumentation.getObjectSize(obj);
        total += objSizeInBytes;
        Class componentType = clz.getComponentType();
        // this means the object is an array
        if (componentType != null && !isObjectClass) {
          if (!componentType.isPrimitive()) {
            for (Object item : (Object[]) obj) {
              if (item != null) {
                stack.push(item);
              }
            }
          }
        } else {
          // Instrumentation.getObjectSize is shallow and as such we must traverse all fields
          for (; clz != null; clz = clz.getSuperclass()) {
            for (Field field : getFields(clz)) {
              Object childObject = null;
              // synthetic fields can result in references outside the original object we want
              // to traverse and thus lead to traversing the entire JVM. For example if we remove
              // this check when traversing a stage we will at some point find a synthetic field
              // which results in traversing all stages. TestMemoryIsolation was written to detect
              // this case.
              // primitives fields will be included in the above Instrumentation.getObjectSize
              if (field.isSynthetic() || field.getType().isPrimitive()) {
                continue;
              } else if (Modifier.isStatic(field.getModifiers())) {
                // only inspect static fields if the class is owned by the given classloader
                if (!isClassOwnedByClassLoader) {
                  continue;
                }
                try {
                  childObject = field.get(null);
                } catch (Throwable ignored) {
                  // this can throw all kinds of strange errors
                  // thus we don't have a better way to handle this
                  if (ignored instanceof OutOfMemoryError) {
                    throw (OutOfMemoryError) ignored;
                  }
                }
              } else if (!isObjectClass) {
                // if the object is a class, we want to traverse the static fields
                // otherwise we'll get a bunch of errors thrown in the Field.get()
                // call below.
                try {
                  childObject = field.get(obj);
                } catch (Throwable ignored) {
                  // this can throw all kinds of strange errors
                  // thus we don't have a better way to handle this
                  if (ignored instanceof OutOfMemoryError) {
                    throw (OutOfMemoryError) ignored;
                  }
                }
              }
              if (childObject != null) {
                stack.push(childObject);
              }
            }
          }
        }
      }
    }
    return total;
  }

  private Field[] getFields(final Class clz) {
    Field[] result = classToFieldCache.get(clz);
    if (result != null) {
      return result;
    }
    try {
      result = clz.getDeclaredFields();
      for (Field field : result) {
        if (!field.isAccessible()) {
          field.setAccessible(true);
        }
      }
      classToFieldCache.putIfAbsent(clz, result);
      return result;
    } catch (Throwable error) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("Error getting fields from {}: {}", clz.getName(), error.toString(), error);
      }
      // eek too bad there is no better way to do this other than importing the asm library:
      // https://bukkit.org/threads/reflecting-fields-that-have-classes-from-unloaded-plugins.160959/
      if (error instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) error;
      }
    }
    return EMPTY_FIELD_ARRAY;
  }
}
