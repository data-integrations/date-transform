/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.date;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This is an example of how you can build unit tests for your transform.
 */
public class DateTransformTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
  private static final Schema INPUT2 = Schema.recordOf("input",
                                                       Schema.Field.of("a",
                                                                       Schema.nullableOf(Schema.of(Schema.Type.LONG))));
  private static final Schema INPUT3 = Schema.recordOf("input",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.LONG)));
  private static final Schema INPUT4 =
    Schema.recordOf("input", Schema.Field.of("a", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  private static final Schema INVALID_INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.BOOLEAN)));

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT2 = Schema.recordOf("output",
                                                        Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT3 =
    Schema.recordOf("output", Schema.Field.of("a", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT4 =
    Schema.recordOf("output", Schema.Field.of("a", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  @Test
  public void testDateTransform() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "MM/dd/yy",
                                                         "b", "yyyy-MM-dd",
                                                         null, OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
    Date date = new Date(System.currentTimeMillis());
    DateFormat df1 = new SimpleDateFormat("MM/dd/yy");

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", df1.format(date))
                          .build(), emitter);

    DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
    Assert.assertEquals(df2.format(date), emitter.getEmitted().get(0).get("b"));
  }

  @Test
  public void testDateTransformLong() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "MM/dd/yy",
                                                               "b", "yyyy-MM-dd",
                                                               "Seconds", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);

    long ts = System.currentTimeMillis();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT2)
            .set("a", ts / 1000)
            .build(), emitter);
    DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
    Assert.assertEquals(df2.format(new Date(ts)), emitter.getEmitted().get(0).get("b"));
  }

    @Test
    public void testDateTransformMultipleFields() throws Exception {
        DateTransformConfig config = new DateTransformConfig("a,b", "MM/dd/yy",
                                                                   "c,d", "yyyy-MM-dd",
                                                                   "Seconds", OUTPUT2.toString());
        Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
        transform.initialize(null);

        long ts = System.currentTimeMillis();
        MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
        transform.transform(StructuredRecord.builder(INPUT3)
                .set("a", ts / 1000)
                .set("b", ts / 1000)
                .build(), emitter);
        DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
        Assert.assertEquals(df2.format(new Date(ts)), emitter.getEmitted().get(0).get("c"));
        Assert.assertEquals(df2.format(new Date(ts)), emitter.getEmitted().get(0).get("d"));
    }

  @Test(expected = IllegalArgumentException.class)
  public void testSourceFormatError() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "1234sdfg",
                                                               "b", "yyyy-MM-dd",
                                                               null, OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTargetFormatError() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "yyyy-MM-dd",
                                                               "b", "1234523909r",
                                                               null, OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongFieldTransform() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "MM/dd/yy",
                                                               "b", "yyyy-MM-dd",
                                                               null, OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
    Date date = new Date(System.currentTimeMillis());
    DateFormat df1 = new SimpleDateFormat("MM/dd/yy");

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INVALID_INPUT)
                          .set("a", "true")
                          .build(), emitter);
  }

  @Test
  public void testDateTransformWithNullValues() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "MM/dd/yy",
                                                               "b", "yyyy-MM-dd",
                                                               null, OUTPUT3.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
    Date date = new Date(System.currentTimeMillis());
    DateFormat df1 = new SimpleDateFormat("MM/dd/yy");

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT4)
                          .set("a", df1.format(date))
                          .build(), emitter);
    StructuredRecord errorRecord = StructuredRecord.builder(INPUT4).set("a", null).build();
    transform.transform(errorRecord, emitter);

    DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
    Assert.assertEquals(df2.format(date), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(1, emitter.getErrors().size());
    Assert.assertEquals(errorRecord, emitter.getErrors().get(0).getInvalidRecord());
  }

  @Test
  public void testDateTransformForNullableSchema() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "MM/dd/yy",
                                                               "b", "yyyy-MM-dd",
                                                               null, OUTPUT4.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT4)
                          .set("a", null)
                          .build(), emitter);
    Assert.assertNull(emitter.getEmitted().get(0).get("b"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidData() throws Exception {
    DateTransformConfig config = new DateTransformConfig("a", "MM/dd/yy",
                                                               "b", "yyyy-MM-dd",
                                                               null, OUTPUT4.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new DateTransform(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT4)
                          .set("a", "true")
                          .build(), emitter);
    Assert.fail();
  }
}
