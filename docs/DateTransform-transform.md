# Date Transform

This transform takes a date in either a unix timestamp or a string, and converts it to a formatted string.

**Source Field** The field (or fields separated by commas) that contain the date to format.

**Source Format** The SimpleDateFormat of the source fields. If the source fields are longs, this can be omitted.

**Seconds Or Milliseconds** If the source fields are longs, you can indicate if they are in seconds or milliseconds.

**Target Field** The field (or fields separated by commas) that contain the date to write to in the output schema.

**Target Format** The SimpleDateFormat of the output field.