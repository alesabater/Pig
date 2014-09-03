REGISTER /usr/lib/pig/lib/jyson-1.0.2.jar;
REGISTER '../udf_python/json_parse.py' USING jython AS converter;
REGISTER '../udf_python/euclidean.py' USING jython AS euclidean;

--data = LOAD '$input' AS line:CHARARRAY;
data = LOAD '/user/gtr/data/year=2014/month=07/day=29/hour=*/day*' AS line:CHARARRAY;

--content = FILTER data BY line MATCHES '.*\\"transmit\\":\\"\\d{4}-\\d{2}-\\d{2}.*';
content = FILTER data BY line MATCHES '$regex';

tsv = FOREACH content GENERATE converter.tsvify(line);

four_fields = FOREACH tsv GENERATE FLATTEN($0);

data_grouped_id = GROUP four_fields by id_equipo;

data_grouped_id_2 = FOREACH data_grouped_id{
                        fields_ordered = ORDER four_fields BY gps;
                        GENERATE group, COUNT(fields_ordered.latitud) AS cuenta;
                        }

DUMP data_grouped_id_2;
