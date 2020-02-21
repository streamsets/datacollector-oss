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
package com.streamsets.pipeline.stage.origin.binlog;

import com.streamsets.pipeline.stage.origin.mysql.offset.GtidSourceOffset;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestGtidSourceOffset {
  private String gtidSet1 = UUID.randomUUID() + ":1-10";
  private String gtid1 = UUID.randomUUID() + ":100";
  private String gtid2 = UUID.randomUUID() + ":400";

  @Test
  public void shouldAddIncompleteTransactions() {
    GtidSourceOffset offset = new GtidSourceOffset(gtidSet1);
    assertThat(offset.incompleteTransactionsContain(gtid1, 100), is(false));
    GtidSourceOffset offset1 = offset.withIncompleteTransaction(gtid1, 300);
    assertThat(offset1.getGtid(), is(gtid1));
    assertThat(offset1.getSeqNo(), is(300L));
    assertThat(offset1.incompleteTransactionsContain(gtid1, 100), is(true));
    assertThat(offset1.incompleteTransactionsContain(gtid1, 300), is(true));
    assertThat(offset1.incompleteTransactionsContain(gtid1, 301), is(false));
    assertThat(offset1.incompleteTransactionsContain(gtid2, 100), is(false));
  }

  @Test
  public void shouldFinishTx() {
    GtidSourceOffset offset = new GtidSourceOffset(gtidSet1).withIncompleteTransaction(gtid1, 300);
    assertThat(offset.incompleteTransactionsContain(gtid1, 100), is(true));
    GtidSourceOffset offset1 = offset.finishTransaction(gtid1);
    assertThat(offset1.incompleteTransactionsContain(gtid1, 100), is(false));
  }

  @Test
  public void shouldParseFromString() {
    GtidSourceOffset offset = new GtidSourceOffset(gtidSet1)
        .withIncompleteTransaction(gtid1, 300)
        .withIncompleteTransaction(gtid2, 500);
    String str = offset.format();
    GtidSourceOffset offset1 = GtidSourceOffset.parse(str);
    assertThat(offset1, is(offset));
  }

  @Test
  public void shouldParseFromSimpleString() {
    assertThat(new GtidSourceOffset(gtidSet1), is(GtidSourceOffset.parse(gtidSet1)));
  }
}
