#include <stdio.h>
#include <avro.h>

#define ENUM_IN_RECORD                                                         \
	"{\"type\":\"record\",\"name\":\"EventName\",\"namespace\":\"com."     \
	"company.avro.schemas\",\"fields\":[{\"name\":\"eventname_model\","    \
	"\"type\":{\"type\":\"enum\",\"namespace\":\"com.company.models\","    \
	"\"name\":\"EventName\",\"symbols\":[\"XXXX\"]}}]}"

#define SIMPLE_ENUM                                                            \
	"{\"name\":\"EventName\",\"namespace\":\"com.company.models\","        \
	"\"type\":\"enum\",\"symbols\":[\"XXXX\"]}"

static const char* schemas[] = {
    ENUM_IN_RECORD, SIMPLE_ENUM, NULL,
};

int main(void)
{
	int i;
	for (i = 0; schemas[i]; ++i) {
		const char* current_schema = schemas[i];

		avro_schema_t schema;
		if (avro_schema_from_json_length(
			current_schema, strlen(current_schema), &schema)) {
			fprintf(stderr,
				"Error when parsing the schemas[%i]: %s\n", i,
				avro_strerror());
			return EXIT_FAILURE;
		}

		avro_schema_decref(schema);
	}

	return EXIT_SUCCESS;
}
