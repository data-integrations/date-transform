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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A plugin to convert dates into formatted strings.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("DateTransform")
@Description("A plugin to convert dates into formatted strings.")
public class DateTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DateTransform.class);
  private static final String DEFAULT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

  private final MyConfig config;
  private DateFormat inputFormat;
  private DateFormat outputFormat;
  private Schema outputSchema;
  private List<Schema.Field> outputFields;

  @VisibleForTesting
  public DateTransform(MyConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Output schema cannot be parsed.", e);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    config.validateWithMacros();
    inputFormat = new SimpleDateFormat(config.sourceFormat);
    outputFormat = new SimpleDateFormat(config.targetFormat);
    outputSchema = Schema.parseJson(config.schema);
    outputFields = Schema.parseJson(config.schema).getFields();
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
            Date date = inputFormat.parse(String.valueOf(input.get(sourceField)));
            builder.convertAndSet(targetField, outputFormat.format(date));
          } else {
            throw new IllegalArgumentException("Source field: " + sourceField +
                                                 " must be of type string or long. It is type: " +
                                                 inputFieldType.name());
          }
        }
      } catch (Exception e) {
        if (input.get(sourceField) == null || Strings.isNullOrEmpty(String.valueOf(input.get(sourceField)))) {
          if (outputSchema.getField(targetField).getSchema().isNullable()) {
            builder.set(targetField, null);
          } else {
            emitter.emitError(new InvalidEntry<>(31, e.getStackTrace()[0].toString() + " : " + e.getMessage(), input));
            return;
          }
        } else {
          throw new IllegalArgumentException(String.format("Cannot parse value %s for format %s. %s.",
                                                           input.get(sourceField), config.targetFormat, e.getMessage()),
                                             e);
        }
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Config for {#DateTransform}
   */
  public static class MyConfig extends PluginConfig {
    private static final String SOURCE_FIELD_NAME = "sourceField";
    private static final String SOURCE_FORMAT_NAME = "sourceFormat";
    private static final String TARGET_FIELD_NAME = "targetField";
    private static final String TARGET_FORMAT_NAME = "targetFormat";

    @Name(SOURCE_FIELD_NAME)
    @Description("The field in the input record containing the date. If it is a string, a format must be provided. " +
            "If the field is a long, it is assumed to be a unix timestamp in milliseconds, and no source format is " +
            "needed. Use commas for multiple fields.")
    @Macro
    private String sourceField;

    @Name(SOURCE_FORMAT_NAME)
    @Description("The simple date format for the input field. If the input field is a long, this can be omitted.")
    @Macro
    @Nullable
    private String sourceFormat;

    @Name(TARGET_FIELD_NAME)
    @Description("The field in the output record to put the formatted date. Use commas for multiple fields.")
    @Macro
    private String targetField;

    @Name(TARGET_FORMAT_NAME)
    @Description("The simple date format for the output field.")
    @Macro
    @Nullable
    private String targetFormat;

    @Name("secondsOrMilliseconds")
    @Description("If the source field is a long, is it in seconds or milliseconds?")
    @Nullable
    private String secondsOrMilliseconds;

    @Name("schema")
    @Description("Specifies the schema of the records outputted from this plugin.")
    private String schema;

    public MyConfig() {
      this.sourceFormat = DEFAULT_FORMAT;
      this.targetFormat = DEFAULT_FORMAT;
      this.secondsOrMilliseconds = "Milliseconds";
    }

    @VisibleForTesting
    public MyConfig(String sourceField, @Nullable String sourceFormat,
                    String targetField, @Nullable String targetFormat,
                    @Nullable String secondsOrMilliseconds, String schema) {
      this.sourceField = sourceField;
      this.sourceFormat = sourceFormat;
      this.targetField = targetField;
      this.targetFormat = targetFormat;
      this.secondsOrMilliseconds = (secondsOrMilliseconds == null) ? "Milliseconds" : secondsOrMilliseconds;
      this.schema = schema;
    }

    private boolean isInSeconds() {
      if (secondsOrMilliseconds == null) {
        return false;
      }
      return secondsOrMilliseconds.equals("Seconds");
    }

    private List<String> getSourceFields() {
      String[] sourceFields = sourceField.split(",");
      List<String> stringList = new ArrayList<>();
      for (String field : sourceFields) {
        stringList.add(field.trim());
      }
      return stringList;
    }

    private List<String> getTargetFields() {
      String[] sourceFields = targetField.split(",");
      List<String> stringList = new ArrayList<>();
      for (String field : sourceFields) {
        stringList.add(field.trim());
      }
      return stringList;
    }

    public void validate(Schema inputSchema) throws IllegalArgumentException {
      try {
        Schema outputSchema = Schema.parseJson(schema);
        if (!containsMacro(TARGET_FIELD_NAME)) {
          String[] targetFields = targetField.split(",");
          for (String tf : targetFields) {
            if (outputSchema.getField(tf.trim()) == null) {
              throw new IllegalArgumentException("Target Field must exist in output schema.");
            }
          }
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Output schema cannot be parsed.", e);
      }
      if (!Strings.isNullOrEmpty(sourceFormat)) {
        if (!containsMacro(SOURCE_FORMAT_NAME)) {
          new SimpleDateFormat(sourceFormat);
        }
      }
      if (!Strings.isNullOrEmpty(targetFormat)) {
        if (!containsMacro(SOURCE_FORMAT_NAME)) {
          new SimpleDateFormat(targetFormat);
        }
      }
      if (!Strings.isNullOrEmpty(sourceField)) {
        if (sourceField.contains(",")) {
          if (Strings.isNullOrEmpty(targetField)) {
            throw new IllegalArgumentException("Target field must not be empty.");
          }
          if (sourceField.split(",").length != targetField.split(",").length) {
            throw new IllegalArgumentException("Target and source fields must have the same number of fields.");
          }
        }
      }
      if (inputSchema == null) {
        throw new IllegalArgumentException("Input schema cannot be null.");
      }
      List<String> sourceFields = getSourceFields();
      for (String field : sourceFields) {
        Schema inputFieldSchema = inputSchema.getField(field).getSchema();
        if (inputFieldSchema.isSimpleOrNullableSimple()) {
          Schema.Type inputFieldType = (inputFieldSchema.isNullableSimple())
            ? inputFieldSchema.getNonNullable().getType()
            : inputFieldSchema.getType();
          if (!(inputFieldType == Schema.Type.STRING || inputFieldType == Schema.Type.LONG)) {
            throw new IllegalArgumentException("Source field: " + field +
                                               " must be of type string or long. It is type: " +
                                               inputFieldType.name());
          }
        }
      }
    }

    // TODO: Combine this with other validate function after HYDRATOR-1100 is fixed.
    private void validateWithMacros() throws IllegalArgumentException {
      try {
        Schema outputSchema = Schema.parseJson(schema);
        String[] targetFields = targetField.split(",");
        for (String tf : targetFields) {
          if (outputSchema.getField(tf.trim()) == null) {
            throw new IllegalArgumentException("Target Field must exist in output schema.");
          }
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Output schema cannot be parsed.", e);
      }
      if (!Strings.isNullOrEmpty(sourceFormat)) {
        new SimpleDateFormat(sourceFormat);
      }
      if (!Strings.isNullOrEmpty(targetFormat)) {
        new SimpleDateFormat(targetFormat);
      }
    }
  }
}

