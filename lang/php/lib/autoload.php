<?php
/**
 * On a manual installation you will have to include this autoloader.
 */

include __DIR__ . '/Avro.php';
include __DIR__ . '/AvroDebug.php';
include __DIR__ . '/AvroException.php';
include __DIR__ . '/AvroGMP.php';
include __DIR__ . '/AvroIO.php';
include __DIR__ . '/AvroNotImplementedException.php';
include __DIR__ . '/AvroUtil.php';

include __DIR__ . '/DataFile/AvroDataIO.php';
include __DIR__ . '/DataFile/AvroDataIOException.php';
include __DIR__ . '/DataFile/AvroDataIOReader.php';
include __DIR__ . '/DataFile/AvroDataIOWriter.php';

include __DIR__ . '/Datum/AvroIOBinaryDecoder.php';
include __DIR__ . '/Datum/AvroIOBinaryEncoder.php';
include __DIR__ . '/Datum/AvroIODatumReader.php';
include __DIR__ . '/Datum/AvroIODatumWriter.php';
include __DIR__ . '/Datum/AvroIOSchemaMatchException.php';
include __DIR__ . '/Datum/AvroIOTypeException.php';

include __DIR__ . '/IO/AvroFile.php';
include __DIR__ . '/IO/AvroIOException.php';
include __DIR__ . '/IO/AvroStringIO.php';

include __DIR__ . '/Protocol/AvroProtocol.php';
include __DIR__ . '/Protocol/AvroProtocolMessage.php';
include __DIR__ . '/Protocol/AvroProtocolParseException.php';

include __DIR__ . '/Schema/AvroArraySchema.php';
include __DIR__ . '/Schema/AvroEnumSchema.php';
include __DIR__ . '/Schema/AvroField.php';
include __DIR__ . '/Schema/AvroFixedSchema.php';
include __DIR__ . '/Schema/AvroMapSchema.php';
include __DIR__ . '/Schema/AvroName.php';
include __DIR__ . '/Schema/AvroNamedSchema.php';
include __DIR__ . '/Schema/AvroNamedSchemata.php';
include __DIR__ . '/Schema/AvroPrimitiveSchema.php';
include __DIR__ . '/Schema/AvroRecordSchema.php';
include __DIR__ . '/Schema/AvroSchema.php';
include __DIR__ . '/Schema/AvroSchemaParseException.php';
include __DIR__ . '/Schema/AvroUnionSchema.php';
