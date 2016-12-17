# Date Transform

This transform takes a date in either a unix timestamp or a string, and converts it to a formatted string. (Macro-enabled)

**Source Field** The field (or fields separated by commas) that contain the date to format. (Macro-enabled)

**Source Format** The SimpleDateFormat of the source fields. If the source fields are longs, this can be omitted. For examples, please see the Java [SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html) documentation. (Macro-enabled)

**Source in Seconds or Milliseconds?** If the source fields are longs, you can indicate if they are in seconds or milliseconds.

**Target Field** The field (or fields separated by commas) that contain the date to write to in the output schema. For examples, please see the Java [SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html) documentation. (Macro-enabled)

**Target Format** The SimpleDateFormat of the output field. (Macro-enabled)