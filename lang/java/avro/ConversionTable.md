## Conversion Table for Logical Types



### AvroDate

- getBackingType(): INT
- getConvertedType(): LocalDate
- convertToRawType(Object):
  - Number: The EpochDays, the raw data type
  - LocalDate: The best fitting data type
  - Date: The AvroDate and the Date have the same value 
  - ZonedDateTime: Takes the LocalDate portion
  - Instant: The AvroDate will have the same value as the UTC Instant
  - CharSequence: A string in the format of YYYY-MM-DD the LocalDate.parse(CharSequence) understands


### AvroDecimal

- getBackingType(): ByteBuffer
- getConvertedType(): BigDecimal
- convertToRawType(Object):
  - ByteBuffer: The internal representation of a BigDecimal as byte[]
  - byte[]: The internal representation of a BigDecimal as byte[]
  - GenericFixed: The internal representation of a BigDecimal as byte[]
  - BigDecimal: The best fitting data type
  - Number: Converts the Number via a double to a BigDecimal
  - CharSequence: A string in the format of e.g. 4.2 with the same scale


### AvroTimestampMicros and AvroTimestampMillis

- getBackingType(): LONG
- getConvertedType(): Instant
- convertToRawType(Object):
  - Number: The internal representation as epochMicros/Millis
  - Date: Assumes the Date is a UTC date time
  - ZonedDateTime: The epochMicros/Millis of its UTC equivalent
  - Instant: The best fitting data type
  - String: Converts the string into an Instant and from there to the internal representation


### AvroLocalTimestampMicros and AvroLocalTimestampMillis

- getBackingType(): LONG
- getConvertedType(): LocalDateTime
- convertToRawType(Object):
  - Number: The internal representation as epochMicros/Millis
  - LocalDateTime: The best fitting data type
  - Date: Date and Time are not changed
  - ZonedDateTime: Is converted into a UTC Instant and its date/time is taken
  - Instant: Date and Time are not changed
  - String: Converts the string into an LocalDateTime and from there to the internal representation


### AvroTimeMicros

- getBackingType(): LONG
- getConvertedType(): LocalTime
- convertToRawType(Object):
  - Number: The internal representation as microseconds from midnight
  - LocalTime: The best fitting data type
  - Instant: Extracts the time from the UTC Instant
  - Date: Extracts the time from Date
  - String: Converts the string into a LocalTime and from there to the internal representation


### AvroTimeMillis

- getBackingType(): INT
- getConvertedType(): LocalTime
- convertToRawType(Object):
  - Number: The internal representation as milliseconds from midnight
  - LocalTime: The best fitting data type
  - Instant: Extracts the time from the UTC Instant
  - Date: Extracts the time from Date
  - String: Converts the string into a LocalTime and from there to the internal representation


### AvroBoolean
- getBackingType(): BOOLEAN
- getConvertedType(): Boolean
- convertToRawType(Object):
  - Boolean: The best fitting data type
  - String: Translates TRUE and FALSE into a Boolean (ignoreCase)
  - Number: Translates 1 and 0 to a Boolean


### AvroFloat
- getBackingType(): FLOAT
- getConvertedType(): Float
- convertToRawType(Object):
  - Float: The best fitting data type
  - String: Translates the string into a float via Float.parse(string)
  - Number: Translates the number into a float using Number.floatValue()
  

### AvroInt
- getBackingType(): INTEGER
- getConvertedType(): Integer
- convertToRawType(Object):
  - Integer: The best fitting data type
  - String: Translates the string into a float via Integer.parse(string)
  - Number: Translates the number into a float using Number.intValue()


### AvroLong
- getBackingType(): LONG
- getConvertedType(): Long
- convertToRawType(Object):
  - Long: The best fitting data type
  - String: Translates the string into a float via Long.parse(string)
  - Number: Translates the number into a float using Number.longValue()


### AvroByte
- getBackingType(): INTEGER
- getConvertedType(): Byte
- convertToRawType(Object):
  - Byte: The best fitting data type
  - String: Translates the string into a float via Integer.parse(string)
  - Number: Translates the number into a float using Number.intValue()

### AvroShort
- getBackingType(): INTEGER
- getConvertedType(): Short
- convertToRawType(Object):
  - Short: The best fitting data type
  - String: Translates the string into a float via Integer.parse(string)
  - Number: Translates the number into a float using Number.intValue()




