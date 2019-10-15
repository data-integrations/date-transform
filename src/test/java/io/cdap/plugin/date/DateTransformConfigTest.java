/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DateTransformConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("inputDate1",
                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("inputDate2",
                                    Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  private static final Schema OUTPUT_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("result1",
                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("result2",
                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  private static final DateTransformConfig VALID_CONFIG =
    new DateTransformConfig("inputDate1, inputDate2", "yyyy.MM.dd G 'at' HH:mm:ss z",
                            "result1, result2", "yyyyy.MMMMM.dd GGG hh:mm aaa",
                            "Milliseconds", OUTPUT_SCHEMA.toString());

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, VALID_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testTargetFieldNotPresent() {
    Schema outputSchema =
      Schema.recordOf("schema",
                      Schema.Field.of("result1", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    DateTransformConfig config = DateTransformConfig.builder(VALID_CONFIG)
      .setSchema(outputSchema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, DateTransformConfig.PROPERTY_TARGET_FIELDS);
  }

  @Test
  public void testSourceFieldWrongType() {
    Schema schema =
      Schema.recordOf("schema",
                      Schema.Field.of("inputDate1",
                                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("inputDate2",
                                      Schema.nullableOf(Schema.of(Schema.Type.BYTES))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, schema);
    assertValidationFailed(failureCollector, DateTransformConfig.PROPERTY_SOURCE_FIELDS);
  }

  @Test
  public void testInvalidSourceFieldNotPresent() {
    DateTransformConfig config = DateTransformConfig.builder(VALID_CONFIG)
      .setSourceFields("inputDate1,nonExisting2")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, DateTransformConfig.PROPERTY_SOURCE_FIELDS);
  }

  @Test
  public void testTargetAndSourceFieldsDifferentCountFormat() {
    DateTransformConfig config = DateTransformConfig.builder(VALID_CONFIG)
      .setTargetFields("result1")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    Set<String> expectedCauses = new HashSet<>(ImmutableList.<String>builder()
                                                 .add(DateTransformConfig.PROPERTY_SOURCE_FIELDS)
                                                 .add(DateTransformConfig.PROPERTY_TARGET_FIELDS)
                                                 .build());
    assertValidationFailed(failureCollector, expectedCauses);
  }

  @Test
  public void testInvalidSourceFormat() {
    DateTransformConfig config = DateTransformConfig.builder(VALID_CONFIG)
      .setSourceFormat("invalidFormat")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, DateTransformConfig.PROPERTY_SOURCE_FORMAT);
  }

  @Test
  public void testInvalidTargetFormat() {
    DateTransformConfig config = DateTransformConfig.builder(VALID_CONFIG)
      .setTargetFormat("invalidFormat")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, DateTransformConfig.PROPERTY_TARGET_FORMAT);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, Set<String> params) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(params.size(), causeList.size());

    Set<String> actualParams = causeList.stream()
      .map((cause) -> cause.getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toSet());
    Assert.assertEquals(params, actualParams);
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
