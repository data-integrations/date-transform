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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * A plugin to convert dates into formatted strings.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("DateTransform")
@Description("A plugin to convert dates into formatted strings.")
public class DateTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DateTransform.class);

  private final DateTransformConfig config;
  private DateFormat inputFormat;
  private DateFormat outputFormat;
  private Schema outputSchema;
  private List<Schema.Field> outputFields;

  @VisibleForTesting
  public DateTransform(DateTransformConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);

    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);

    FailureCollector failureCollector = context.getFailureCollector();
    Schema inputSchema = context.getInputSchema();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    inputFormat = new SimpleDateFormat(config.getSourceFormat());
    outputFormat = new SimpleDateFormat(config.getTargetFormat());
    outputSchema = config.getSchema();
    outputFields = outputSchema.getFields();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    List<String> sourceFields = config.getSourceFields();
    List<String> targetFields = config.getTargetFields();

    for (Schema.Field field : outputFields) {
      String name = field.getName();
      if (input.get(name) != null && !targetFields.contains(name)) {
        builder.set(name, input.get(name));
      }
    }
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String targetField = targetFields.get(i);

      if (input.getSchema().getField(sourceField) == null) {
        continue;
      }
      Schema inputFieldSchema = input.getSchema().getField(sourceField).getSchema();
      try {
        if (inputFieldSchema.isSimpleOrNullableSimple()) {
          Schema.Type inputFieldType = (inputFieldSchema.isNullableSimple())
            ? inputFieldSchema.getNonNullable().getType()
            : inputFieldSchema.getType();
          if (inputFieldType == Schema.Type.LONG) {
            long ts = input.get(sourceField);
            if (config.isInSeconds()) {
              ts *= 1000;
            }
            Date date = new Date(ts);
            builder.convertAndSet(targetField, outputFormat.format(date));
          } else if (inputFieldType == Schema.Type.STRING) {
            Date date = inputFormat.parse(input.get(sourceField));
            builder.convertAndSet(targetField, outputFormat.format(date));
          } else {
            throw new IllegalArgumentException("Source field: " + sourceField +
                                                 " must be of type string or long. It is type: " +
                                                 inputFieldType.name());
          }
        }
      } catch (Exception e) {
        if (input.get(sourceField) == null || Strings.isNullOrEmpty(input.get(sourceField))) {
          if (outputSchema.getField(targetField).getSchema().isNullable()) {
            builder.set(targetField, null);
          } else {
            emitter.emitError(new InvalidEntry<>(31, e.getStackTrace()[0].toString() +
              " : " + e.getMessage(), input));
            return;
          }
        } else {
          throw new IllegalArgumentException(String.format("Cannot parse value %s for format %s. %s.",
                                                           input.get(sourceField), config.getTargetFormat(),
                                                           e.getMessage()), e);
        }
      }
    }
    emitter.emit(builder.build());
  }
}

