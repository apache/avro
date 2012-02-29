#include <avro.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define SIMPLE_ARRAY \
"{\"type\": \"array\", \"items\": \"long\"}"


int main(void)
{
  avro_schema_t schema = NULL;
  avro_schema_error_t error;
  avro_value_iface_t *simple_array_class;
  avro_value_t simple;

  /* Initialize the schema structure from JSON */
  if (avro_schema_from_json(SIMPLE_ARRAY, sizeof(SIMPLE_ARRAY),
                            &schema, &error)) {
    fprintf(stdout, "Unable to parse schema\n");
    exit(EXIT_FAILURE);
  }

  // Create avro class and value
  simple_array_class = avro_generic_class_from_schema( schema );
  if ( simple_array_class == NULL )
  {
    fprintf(stdout, "Unable to create simple array class\n");
    exit(EXIT_FAILURE);
  }

  if ( avro_generic_value_new( simple_array_class, &simple ) )
  {
    fprintf(stdout, "Error creating instance of record\n" );
    exit(EXIT_FAILURE);
  }

  // Release the avro class and value
  avro_value_decref( &simple );
  avro_value_iface_decref( simple_array_class );
  avro_schema_decref(schema);
  
  return 0;

}
