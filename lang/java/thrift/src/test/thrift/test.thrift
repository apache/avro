namespace java org.apache.avro.thrift.test

enum E {
  X = 1,
  Y = 2,
  Z = 3,
}

struct Nested {
  1: i32 x
}

// contains each primitive type
struct Test {
  1: bool boolField
  2: byte byteField
  3: i16 i16Field
  4: i32 i32Field
  5: i64 i64Field
  6: double doubleField
  7: string stringField
  8: binary binaryField
  9: map<string,i32> mapField
 10: list<i32> listField
 11: set<i32> setField
 12: E enumField
 13: Nested structField
}

exception Error {
  1: string message,
}

service Foo {

   void ping(),

   i32 add(1:i32 num1, 2:i32 num2),

   oneway void zip(),
}
