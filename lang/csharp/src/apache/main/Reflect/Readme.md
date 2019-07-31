# Namespace Avro.Reflect

This namespace contains classes that implement Avro serialization and deserialization for plain C# objects. The classes use .net reflection to implement the serializers. The interface is similar to the Generic and Specific serialiation classes.

## Serialization

The approach starts with the schema and interates both the schema and the dotnet object together in a depth first manner per the specification. Serialization is the same as the Generic serializer except where the serializer encounters:
- *A fixed type*: if the corresponding dotnet object type is a byte[] of the correct length then the object is serialized, otherwise an exception is thrown.
- *A record type*: the serializer matches the schema property name to the dotnet object property name and then reursively serializes the schema property and the dotnet object property
- *An array type*: See array serialization/deserialization.

Basic serialization is performed as in the following example:

```csharp
    Schema schema; // created previously
    T myObject; // created previously


    var avroWriter = new ReflectWriter<T>(schema);
    using (var stream = new MemoryStream(256))
    {
        avroWriter.Write(myObject, new BinaryEncoder(stream));
    }
```

## Deserialization

Deserialization proceeds in much the same fashion as serialization. When required objects are created. By default this is with:
```csharp
    Activator.CreateInstance(x);
```
however this can be overridden by setting the deserializer property RecordFactory. 
```csharp
public Func<Type, object> RecordFactory {get;set;}
```
You might want to do this if your class contains interfaces and/or if you use an IoC container.

See the section on Arrays. The ArrayHelper specifies the type of object created when an array is deserialized. The default is List<T>.

The type created for Map objects is specified by the Deserializer property MapType. *This must be a two (or more) parameter generic type where the first type paramater is string and the second is undefined* e.g. List<string,>. 
```csharp
public Type MapType { get; set; }
```
By default the MapType is List<string,>
```

Basic deserialization is performed as in the following example:

```csharp
    Schema schema; // created previously

    // using same writer and reader schema in this example.
    var avroReader = new ReflectReader<T>(schema, schema);

    using (var stream = new MemoryStream(serialized))
    {
        deserialized = avroReader.Read(null, new BinaryDecoder(stream));
    }
```

## Class cache

The dotnet reflection libraries can add an amount of performance overhead. Efforts are made to minimize this by supporting a cache of class details obtained by reflection (PropertyInfo objects) so that property value lookups can be performed quickly and with as little overhead as possible. 

The class cache can be created separately from the serializer/deserializer and reused.

```csharp
    var cache = new ClassCache();
    var writer = new ReflectWriter<MultiList>(schema, cache);
    var reader = new ReflectReader<MultiList>(schema, schema, cache);
```
The class cache is also used with default type conversions and with array serialization and deserialization.

## Converters

Converters are classes that convert to and from Avro primitive types and dotnet types. An example of where converters are used is to convert between dotnet DateTimeOffet object and the chosen Avro primitive. 

Converters are implemented by inheriting from TypedFieldConverter<byte[],GenericFixed>, or creating an object of type FuncFieldConverter<A,T>.

_Example TypedFieldConverter_:

```csharp
        public class GenericFixedConverter : TypedFieldConverter<byte[],GenericFixed>
        {
            public override GenericFixed From(byte[] o, Schema s)
            {
                return new GenericFixed(s as FixedSchema, o);
            }

            public override byte[] To(GenericFixed o, Schema s)
            {
                return o.Value;
            }
        }
```

### Specifying Converters in Attributes

```csharp
    public class LogMessage
    {

        [AvroField(typeof(DateTimeOffsetToLongConverter))]
        public DateTimeOffset TimeStamp { get; set; }

    }
```

### Default Converters

Default converters are defined to convert between an Avro primitive and C# type without explicitly defining the converter for a field. Default converters are static and are registered with the class cache.

```csharp
    ClassCache.AddDefaultConverter<byte[], GenericFixed>((a,s)=>new GenericFixed(s as FixedSchema, a), (p,s)=>p.Value);
    var writer = new ReflectWriter<GenericFixedRec>(schema);
    var reader = new ReflectReader<GenericFixedRec>(schema, schema);

```
## Attributes

The AvroField attribute can be used to defined field converters or to change the name of the dotnet property (or both).

```csharp
    public class LogMessage
    {
        [AvroField("message")]
        public string Message { get; set; }

        [AvroField(typeof(DateTimeOffsetToLongConverter))]
        public DateTimeOffset TimeStamp { get; set; }
    }
```

## Arrays

By default the reflect code will serialize and deserialized between Avro arrays and classes that implement IList. Classes that implement IEnumerable but do not implement IList can be handled by implementing an ArrayHelper class. The array helper provides a standard interface for a number of methods needed for serialization and deserialization but which are not supported by IEnumerable.

An additional metadata called "helper" is required in the schema. This acts like the name of a record type and is used to associate a helper with a particular schema array. 

_Example_: ConcurrentQueue


```csharp

    public class ConcurrentQueueHelper<T> : ArrayHelper
    {
        public override int Count()
        {
            ConcurrentQueue<T> e = (ConcurrentQueue<T>)Enumerable;
            return e.Count;
        }

        public override void Add(object o)
        {
            ConcurrentQueue<T> e = (ConcurrentQueue<T>)Enumerable;
            e.Enqueue((T)o);
        }

        public override void Clear()
        {
            ConcurrentQueue<T> e = (ConcurrentQueue<T>)Enumerable;
#if NET461
            while (e.TryDequeue(out _)) { }
#else
            e.Clear();
#endif
        }

        public override Type ArrayType
        {
            get => typeof(ConcurrentQueue<>);
        }

        public ConcurrentQueueHelper(IEnumerable enumerable) : base(enumerable)
        {
            Enumerable = enumerable;
        }
    }

    string recordList = @"
    {
        ""type"": ""array"",
        ""helper"": ""recordListQueue"",
        ""items"": ""string""
    }"

    // using the helper

    var schema = Schema.Parse(recordList);
    var fixedRecWrite = new ConcurrentQueue<string>();

    var cache = new ClassCache();
    cache.AddArrayHelper("recordListQueue", typeof(ConcurrentQueueHelper<string>));

    var writer = new ReflectWriter<ConcurrentQueue<ConcurrentQueueRec>>(schema, cache);
    var reader = new ReflectReader<ConcurrentQueue<ConcurrentQueueRec>>(schema, schema, cache);

```
