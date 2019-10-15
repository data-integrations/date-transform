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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Config for {@link DateTransform}
 */
public class DateTransformConfig extends PluginConfig {
  public static final String PROPERTY_SOURCE_FIELDS = "sourceFields";
  public static final String PROPERTY_SOURCE_FORMAT = "sourceFormat";
  public static final String PROPERTY_TARGET_FIELDS = "targetFields";
  public static final String PROPERTY_TARGET_FORMAT = "targetFormat";

  private static final String TIME_MILLISECONDS = "Milliseconds";
  private static final String TIME_SECONDS = "Milliseconds";

  @Name(PROPERTY_SOURCE_FIELDS)
  @Description("The field in the input record containing the date. If it is a string, a format must be provided. " +
    "If the field is a long, it is assumed to be a unix timestamp in milliseconds, and no source format is " +
    "needed. Use commas for multiple fields.")
  @Macro
  private String sourceFields;

  @Name(PROPERTY_SOURCE_FORMAT)
  @Description("The simple date format for the input field. If the input field is a long, this can be omitted.")
  @Macro
  private String sourceFormat;

  @Name(PROPERTY_TARGET_FIELDS)
  @Description("The field in the output record to put the formatted date. Use commas for multiple fields.")
  @Macro
  private String targetFields;

  @Name(PROPERTY_TARGET_FORMAT)
  @Description("The simple date format for the output field.")
  @Macro
  private String targetFormat;

  @Name("secondsOrMilliseconds")
  @Description("If the source field is a long, is it in seconds or milliseconds?")
  private String secondsOrMilliseconds;

  @Name("schema")
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  @VisibleForTesting
  public DateTransformConfig(String sourceFields, @Nullable String sourceFormat,
                             String targetFields, @Nullable String targetFormat,
                             @Nullable String secondsOrMilliseconds, String schema) {
    this.sourceFields = sourceFields;
    this.sourceFormat = sourceFormat;
    this.targetFields = targetFields;
    this.targetFormat = targetFormat;
    this.secondsOrMilliseconds = (secondsOrMilliseconds == null) ? TIME_MILLISECONDS : secondsOrMilliseconds;
    this.schema = schema;
  }

  private DateTransformConfig(Builder builder) {
    this.sourceFields = builder.sourceFields;
    this.sourceFormat = builder.sourceFormat;
    this.targetFields = builder.targetFields;
    this.targetFormat = builder.targetFormat;
    this.secondsOrMilliseconds = builder.secondsOrMilliseconds;
    this.schema = builder.schema;
  }

  public String getSourceFormat() {
    return sourceFormat;
  }

  public String getTargetFormat() {
    return targetFormat;
  }

  public boolean isInSeconds() {
    if (secondsOrMilliseconds == null) {
      return false;
    }
    return secondsOrMilliseconds.equals(TIME_SECONDS);
  }

  public String getSecondsOrMilliseconds() {
    return secondsOrMilliseconds;
  }

  public List<String> getSourceFields() {
    String[] sourceFields = this.sourceFields.split(",");
    List<String> stringList = new ArrayList<>();
    for (String field : sourceFields) {
      stringList.add(field.trim());
    }
    return stringList;
  }

  public List<String> getTargetFields() {
    String[] sourceFields = targetFields.split(",");
    List<String> stringList = new ArrayList<>();
    for (String field : sourceFields) {
      stringList.add(field.trim());
    }
    return stringList;
  }

  public String getTargetFieldsString() {
    return targetFields;
  }

  public String getSourceFieldsString() {
    return sourceFields;
  }

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    try {
      Schema outputSchema = getSchema();
      if (!containsMacro(PROPERTY_TARGET_FIELDS)) {
        List<String> targetFields = this.getTargetFields();
        for (String targetField : targetFields) {
          if (outputSchema.getField(targetField) == null) {
            failureCollector.addFailure(String.format("Target field '%s' is not present in output schema.",
                                                      targetField), null)
              .withConfigElement(PROPERTY_TARGET_FIELDS, targetField)
              .withOutputSchemaField(targetField);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure("Output schema cannot be parsed.", null)
        .withStacktrace(e.getStackTrace());
    }

    validateDateFormat(failureCollector, targetFormat, PROPERTY_TARGET_FORMAT);
    validateDateFormat(failureCollector, sourceFormat, PROPERTY_SOURCE_FORMAT);

    if (!containsMacro(PROPERTY_TARGET_FIELDS) && !containsMacro(PROPERTY_SOURCE_FIELDS)) {
      if (this.getTargetFields().size() != this.getSourceFields().size()) {
        failureCollector.addFailure("Target and source fields must contain the same number of fields.", null)
          .withConfigProperty(PROPERTY_SOURCE_FIELDS)
          .withConfigProperty(PROPERTY_TARGET_FIELDS);
      }
    }

    if (inputSchema == null) {
      failureCollector.addFailure("Input schema cannot be null.", null);
      throw failureCollector.getOrThrowException();
    }

    List<String> sourceFields = getSourceFields();
    for (String field : sourceFields) {
      Schema.Field inputField = inputSchema.getField(field);
      if (inputField == null) {
        failureCollector.addFailure(String.format("Source field '%s' is not present in input schema.",
                                                  field), null)
          .withConfigProperty(PROPERTY_SOURCE_FIELDS)
          .withInputSchemaField(field);
      } else {
        Schema inputFieldSchema = inputField.getSchema();

        if (inputFieldSchema.isNullable()) {
          inputFieldSchema = inputFieldSchema.getNonNullable();
        }

        if (inputFieldSchema.getLogicalType() != null || (inputFieldSchema.getType() != Schema.Type.STRING &&
          inputFieldSchema.getType() != Schema.Type.LONG)) {
          failureCollector.addFailure(String.format("Source Field '%s' is unexpected type '%s'.",
                                                    inputField.getName(), inputFieldSchema.getDisplayName()),
                                      "Supported types are 'string' or 'long'.")
            .withConfigProperty(PROPERTY_SOURCE_FIELDS)
            .withInputSchemaField(field);
        }
      }
    }
  }

  public void validateDateFormat(FailureCollector failureCollector, String value, String propertyName) {
    if (!containsMacro(propertyName)) {
      try {
        new SimpleDateFormat(value);
      } catch (IllegalArgumentException ex) {
        failureCollector.addFailure(String.format("Field '%s' contains invalid date pattern '%s'.",
                                                  propertyName, value), "Use format specified in " +
          "https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html.")
          .withConfigProperty(propertyName)
          .withStacktrace(ex.getStackTrace());
      }
    }
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema: " + schema, e);
    }
  }

  public String getSchemaString() {
    return schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(DateTransformConfig copy) {
    return new Builder()
      .setSourceFields(copy.getSourceFieldsString())
      .setSourceFormat(copy.getSourceFormat())
      .setTargetFields(copy.getTargetFieldsString())
      .setTargetFormat(copy.getTargetFormat())
      .setSecondsOrMilliseconds(copy.getSecondsOrMilliseconds())
      .setSchema(copy.getSchemaString());
  }

  /**
   * Builder for {@link DateTransformConfig}
   */
  public static final class Builder {
    private String sourceFields;
    private String sourceFormat;
    private String targetFields;
    private String targetFormat;
    private String secondsOrMilliseconds;
    private String schema;

    public Builder setSourceFields(String sourceFields) {
      this.sourceFields = sourceFields;
      return this;
    }

    public Builder setSourceFormat(String sourceFormat) {
      this.sourceFormat = sourceFormat;
      return this;
    }

    public Builder setTargetFields(String targetFields) {
      this.targetFields = targetFields;
      return this;
    }

    public Builder setTargetFormat(String targetFormat) {
      this.targetFormat = targetFormat;
      return this;
    }

    public Builder setSecondsOrMilliseconds(String secondsOrMilliseconds) {
      this.secondsOrMilliseconds = secondsOrMilliseconds;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    private Builder() {
    }

    public DateTransformConfig build() {
      return new DateTransformConfig(this);
    }
  }
}
