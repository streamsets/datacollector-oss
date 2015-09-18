package com.streamsets.datacollector.memory;
import java.util.*;
/*
 * This code has been copied from HPPC and is Apache Licensed.
 *
 *  Licensed under the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
/**
 * A hash set of <code>int</code>s, implemented using using open addressing
 * with linear probing for collision resolution.
 */
@javax.annotation.Generated(
  date = "2015-03-22T16:40:44-0700",
  value = "KTypeOpenHashSet.java")
class IntOpenHashSet {

  protected static final
  int
    EMPTY_KEY =
    0      ;

  /** The hash array holding keys. */
  public   int []
    keys;

  /**
   * The number of stored keys (assigned key slots), excluding the special
   * "zero" key, if any.
   *
   * @see #size()
   * @see #hasEmptyKey
   */
  protected int assigned;

  /**
   * Mask for slot scans in {@link #keys}.
   */
  protected int mask;

  /**
   * We perturb hash values with a container-unique
   * seed to avoid problems with nearly-sorted-by-hash
   * values on iterations.
   *
   * @see #hashKey
   * @see "http://issues.carrot2.org/browse/HPPC-80"
   * @see "http://issues.carrot2.org/browse/HPPC-103"
   */
  protected int keyMixer;

  /**
   * Expand (rehash) {@link #keys} when {@link #assigned} hits this value.
   */
  protected int resizeAt;

  /**
   * Special treatment for the "empty slot" key marker.
   */
  protected boolean hasEmptyKey;

  /**
   * The load factor for {@link #keys}.
   */
  protected double loadFactor;

  /**
   * Per-instance hash order mixing strategy.
   * @see #keyMixer
   */
  protected XorShiftRandom orderMixer;

  public IntOpenHashSet(int expectedElements) {
    this(expectedElements, HashContainers.DEFAULT_LOAD_FACTOR);
  }

  public IntOpenHashSet(int expectedElements, double loadFactor) {
    this.orderMixer = new XorShiftRandom();
    this.loadFactor = verifyLoadFactor(loadFactor);
    ensureCapacity(expectedElements);
  }

  public boolean add(int key) {
    if (((key) == 0)) {
      boolean hadEmptyKey = hasEmptyKey;
      hasEmptyKey = true;
      return hadEmptyKey;
    } else {
      final int [] keys =  this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((key) == (existing))) {
          return false;
        }
        slot = (slot + 1) & mask;
      }

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(slot, key);
      } else {
        keys[slot] = key;
      }

      assigned++;
      return true;
    }
  }

  /**
   * Adds all elements from the given list (vararg) to this set.
   *
   * @return Returns the number of elements actually added as a result of this
   *         call (not previously present in the set).
   */
  /*  */
  public final int addAll(int... elements) {
    ensureCapacity(elements.length);
    int count = 0;
    for (int e : elements) {
      if (add(e)) {
        count++;
      }
    }
    return count;
  }

  public boolean contains(int key) {
    if (((key) == 0)) {
      return hasEmptyKey;
    } else {
      final int [] keys =  this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;
      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((key) == (existing))) {
          return true;
        }
        slot = (slot + 1) & mask;
      }
      return false;
    }
  }

  public void clear() {
    assigned = 0;
    hasEmptyKey = false;
    Arrays.fill(keys,  EMPTY_KEY);
  }

  /**
   * Ensure this container can hold at least the
   * given number of elements without resizing its buffers.
   *
   * @param expectedElements The memoryConsumed number of elements, inclusive.
   */
  public void ensureCapacity(int expectedElements) {
    if (expectedElements > resizeAt || keys == null) {
      final int[] prevKeys =  this.keys;
      allocateBuffers(HashContainers.minBufferSize(expectedElements, loadFactor));
      if (prevKeys != null && !isEmpty()) {
        rehash(prevKeys);
      }
    }
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int size() {
    return assigned + (hasEmptyKey ? 1 : 0);
  }

  /**
   * Returns a hash code for the given key.
   *
   * The default implementation mixes the hash of the key with {@link #keyMixer}
   * to differentiate hash order of keys between hash containers. Helps
   * alleviate problems resulting from linear conflict resolution in open
   * addressing.
   *
   * The output from this function should evenly distribute keys across the
   * entire integer range.
   */
  protected int hashKey(int key) {
    assert !((key) == 0); // Handled as a special case (empty slot marker).
    return BitMixer.mix(key, this.keyMixer);
  }

  /**
   * Validate load factor range and return it. Override and suppress if you need
   * insane load factors.
   */
  protected double verifyLoadFactor(double loadFactor) {
    HashContainers.checkLoadFactor(loadFactor, HashContainers.MIN_LOAD_FACTOR, HashContainers.MAX_LOAD_FACTOR);
    return loadFactor;
  }

  /**
   * Rehash from old buffers to new buffers.
   */
  protected void rehash(int[] fromKeys) {
    // Rehash all stored keys into the new buffers.
    final int[] keys =  this.keys;
    final int mask = this.mask;
    int existing;
    for (int i = fromKeys.length; --i >= 0;) {
      if (!((existing = fromKeys[i]) == 0)) {
        int slot = hashKey(existing) & mask;
        while (!((keys[slot]) == 0)) {
          slot = (slot + 1) & mask;
        }
        keys[slot] = existing;
      }
    }
  }

  /**
   * Allocate new internal buffers. This method attempts to allocate
   * and assign internal buffers atomically (either allocations succeed or not).
   */
  protected void allocateBuffers(int arraySize) {
    assert Integer.bitCount(arraySize) == 1;

    // Compute new hash mixer candidate before expanding.
    final int newKeyMixer = this.orderMixer.next(arraySize);

    // Ensure no change is done if we hit an OOM.
    int[] prevKeys =  this.keys;
    try {
      this.keys = (new int [arraySize]);
    } catch (OutOfMemoryError e) {
      this.keys = prevKeys;
      throw new OutOfMemoryError("Not enough memory to allocate buffers for rehashing: " + e);
    }

    this.resizeAt = HashContainers.expandAtCount(arraySize, loadFactor);
    this.keyMixer = newKeyMixer;
    this.mask = arraySize - 1;
  }

  /**
   * This method is invoked when there is a new key to be inserted into
   * the buffer but there is not enough empty slots to do so.
   *
   * New buffers are allocated. If this succeeds, we know we can proceed
   * with rehashing so we assign the pending element to the previous buffer
   * (possibly violating the invariant of having at least one empty slot)
   * and rehash all keys, substituting new buffers at the end.
   */
  protected void allocateThenInsertThenRehash(int slot, int pendingKey) {
    assert assigned == resizeAt
      && (( keys[slot]) == 0);

    // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
    final int[] prevKeys =  this.keys;
    allocateBuffers(HashContainers.nextBufferSize(keys.length, assigned, loadFactor));
    assert this.keys.length > prevKeys.length;

    // We have succeeded at allocating new data so insert the pending key/value at
    // the free slot in the old arrays before rehashing.
    prevKeys[slot] = pendingKey;

    // Rehash old keys, including the pending key.
    rehash(prevKeys);
  }

  /**
   * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
   */
  protected void shiftConflictingKeys(int gapSlot) {
    final int[] keys =  this.keys;
    final int mask = this.mask;

    // Perform shifts of conflicting keys to fill in the gap.
    int distance = 0;
    while (true) {
      final int slot = (gapSlot + (++distance)) & mask;
      final int existing = keys[slot];
      if (((existing) == 0)) {
        break;
      }

      final int idealSlot = hashKey(existing);
      final int shift = (slot - idealSlot) & mask;
      if (shift >= distance) {
        // Entry at this position was originally at or before the gap slot.
        // Move the conflict-shifted entry to the gap's position and repeat the procedure
        // for any entries to the right of the current position, treating it
        // as the new gap.
        keys[gapSlot] = existing;
        gapSlot = slot;
        distance = 0;
      }
    }

    // Mark the last found gap slot without a conflict as empty.
    keys[gapSlot] =  EMPTY_KEY;
  }

  /**
   * Key hash bit mixing.
   */
  private static final class BitMixer {
    // Don't bother mixing very small key domains much.
    static int mix (byte key)   { return key * 0x85ebca6b; }
    static int mix0(byte key)   { return key * 0x85ebca6b; }

    static int mix (short key)  { int k = key * 0x85ebca6b; return k ^= k >>> 13; }
    static int mix0(short key)  { int k = key * 0x85ebca6b; return k ^= k >>> 13; }
    static int mix (char  key)  { int k = key * 0x85ebca6b; return k ^= k >>> 13; }
    static int mix0(char  key)  { int k = key * 0x85ebca6b; return k ^= k >>> 13; }

    static int mix (int key)    { return murmurHash3(key); }
    static int mix0(int key)    { return murmurHash3(key); }

    static int mix (float key)  { return murmurHash3(Float.floatToIntBits(key)); }
    static int mix0(float key)  { return murmurHash3(Float.floatToIntBits(key)); }

    static int mix (double key) { long v = Double.doubleToLongBits(key); return murmurHash3((int)((v >>> 32) ^ v)); }
    static int mix0(double key) { long v = Double.doubleToLongBits(key); return murmurHash3((int)((v >>> 32) ^ v)); }

    static int mix (Object key) { return murmurHash3(key.hashCode()); }
    static int mix0(Object key) { return key == null ? 0 : murmurHash3(key.hashCode()); }

    static int mix (byte key, int seed)  { return mix(key ^ seed); }
    static int mix0(byte key, int seed)  { return mix0(key ^ seed); }

    static int mix (short key, int seed) { return mix(key ^ seed); }
    static int mix0(short key, int seed) { return mix0(key ^ seed); }
    static int mix (char key, int seed)  { return mix(key ^ seed); }
    static int mix0(char key, int seed)  { return mix0(key ^ seed); }

    static int mix (int key, int seed)   { return mix0(key ^ seed); }
    static int mix0(int key, int seed)   { return mix0(key ^ seed); }

    static int mix (float key, int seed)  { return murmurHash3(Float.floatToIntBits(key) ^ seed); }
    static int mix0(float key, int seed)  { return murmurHash3(Float.floatToIntBits(key) ^ seed); }

    static int mix (double key, int seed)  { long v = Double.doubleToLongBits(key); return murmurHash3((int)((v >>> 32) ^ v) ^ seed); }
    static int mix0(double key, int seed)  { long v = Double.doubleToLongBits(key); return murmurHash3((int)((v >>> 32) ^ v) ^ seed); }

    static int mix (Object key, int seed)  { return murmurHash3(key.hashCode() ^ seed); }
    static int mix0(Object key, int seed)  { return key == null ? 0 : murmurHash3(key.hashCode() ^ seed); }

    /** */
    private static int murmurHash3(int k) {
      k ^= k >>> 16;
      k *= 0x85ebca6b;
      k ^= k >>> 13;
      k *= 0xc2b2ae35;
      k ^= k >>> 16;
      return k;
    }
  }
  private static final class HashContainers {
    /**
     * Maximum array size for hash containers (power-of-two and still
     * allocable in Java, not a negative int).
     */
    final static int MAX_HASH_ARRAY_LENGTH = 0x80000000 >>> 1;

    /**
     * Minimum hash buffer size.
     */
    final static int MIN_HASH_ARRAY_LENGTH = 4;

    /**
     * Default load factor.
     */
    final static float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Minimal sane load factor (99 empty slots per 100).
     */
    final static float MIN_LOAD_FACTOR = 1 / 100.0f;

    /**
     * Maximum sane load factor (1 empty slot per 100).
     */
    final static float MAX_LOAD_FACTOR = 99 / 100.0f;

    /**
     * Compute and return the maximum number of elements (inclusive)
     * that can be stored in a hash container for a given load factor.
     */
    public static int maxElements(double loadFactor) {
      checkLoadFactor(loadFactor, 0, 1);
      return expandAtCount(MAX_HASH_ARRAY_LENGTH, loadFactor) - 1;
    }

    /** */
    static int minBufferSize(int elements, double loadFactor) {
      if (elements < 0) {
        throw new IllegalArgumentException(
          "Number of elements must be >= 0: " + elements);
      }

      long length = (long) Math.ceil(elements / loadFactor);
      if (length == elements) {
        length++;
      }
      length = Math.max(MIN_HASH_ARRAY_LENGTH, nextHighestPowerOfTwo(length));

      if (length > MAX_HASH_ARRAY_LENGTH) {
        throw new IllegalStateException(String.format(
          "Maximum array size exceeded for this load factor (elements: %d, load factor: %f)",
          elements,
          loadFactor));
      }

      return (int) length;
    }

    /** */
    static int nextBufferSize(int arraySize, int elements, double loadFactor) {
      checkValidArraySize(arraySize);
      if (arraySize == MAX_HASH_ARRAY_LENGTH) {
        throw new IllegalStateException(String.format(
          "Maximum array size exceeded for this load factor (elements: %d, load factor: %f)",
          elements,
          loadFactor));
      }

      return (int) arraySize << 1;
    }

    /** */
    static int expandAtCount(int arraySize, double loadFactor) {
      checkValidArraySize(arraySize);
      // Take care of hash container invariant (there has to be at least one empty slot to ensure
      // the lookup loop finds either the element or an empty slot).
      return Math.min(arraySize - 1, (int) Math.ceil(arraySize * loadFactor));
    }

    /** */
    static void checkLoadFactor(double loadFactor, double minAllowedInclusive, double maxAllowedInclusive) {
      if (loadFactor < minAllowedInclusive || loadFactor > maxAllowedInclusive) {
        throw new IllegalStateException(String.format(
          "The load factor should be in range [%.2f, %.2f]: %f",
          minAllowedInclusive,
          maxAllowedInclusive,
          loadFactor));
      }
    }

    /** */
    private static void checkValidArraySize(int arraySize) {
      // These are internals, we can just assert without retrying.
      assert arraySize > 1;
      assert nextHighestPowerOfTwo(arraySize) == arraySize;
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static int nextHighestPowerOfTwo(int v) {
      v--;
      v |= v >> 1;
      v |= v >> 2;
      v |= v >> 4;
      v |= v >> 8;
      v |= v >> 16;
      v++;
      return v;
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static long nextHighestPowerOfTwo(long v) {
      v--;
      v |= v >> 1;
      v |= v >> 2;
      v |= v >> 4;
      v |= v >> 8;
      v |= v >> 16;
      v |= v >> 32;
      v++;
      return v;
    }
  }
  /**
   * XorShift pseudo random number generator. This class is not thread-safe and should be
   * used from a single thread only.
   *
   * @see "http://en.wikipedia.org/wiki/Xorshift"
   * @see "http://www.jstatsoft.org/v08/i14/paper"
   * @see "http://www.javamex.com/tutorials/random_numbers/xorshift.shtml"
   */
  @SuppressWarnings("serial")
  private static class XorShiftRandom extends Random {
    private long x;

    public XorShiftRandom() {
      this(System.nanoTime());
    }

    public XorShiftRandom(long seed) {
      this.setSeed(seed);
    }

    @Override
    public long nextLong() {
      return x = next(x);
    }

    @Override
    protected int next(int bits) {
      return (int) (nextLong() & ((1L << bits) - 1));
    }

    @Override
    public void setSeed(long seed) {
      this.x = seed;
    }

    public static long next(long x) {
      x ^= (x << 21);
      x ^= (x >>> 35);
      x ^= (x << 4);
      return x;
    }
  }
}