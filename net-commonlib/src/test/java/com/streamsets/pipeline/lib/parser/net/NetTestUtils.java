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
package com.streamsets.pipeline.lib.parser.net;

import com.google.common.primitives.Bytes;
import com.streamsets.testing.RandomTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class NetTestUtils {

  public static List<List<Byte>> getRandomByteSlices(byte[] bytes) {
    List<Byte> byteList = Bytes.asList(bytes);

    int numSlices = RandomTestUtils.nextInt(2, 10);
    List<Integer> sliceIndexes = new ArrayList<>();
    for (int i=1; i<=numSlices; i++) {
      sliceIndexes.add(RandomTestUtils.nextInt(0, bytes.length));
    }
    Collections.sort(sliceIndexes);

    List<List<Byte>> slices = new LinkedList<>();

    int byteInd = 0;
    for (int sliceIndex : sliceIndexes) {
      // System.out.println(String.format("Slice from %d through %d", byteInd, sliceIndex));
      slices.add(byteList.subList(byteInd, sliceIndex));
      byteInd = sliceIndex;
    }
    // System.out.println(String.format("Slice from %d through %d", byteInd, byteList.size()));
    slices.add(byteList.subList(byteInd, byteList.size()));

    return slices;
  }

}
