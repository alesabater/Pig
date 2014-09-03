REGISTER /usr/lib/pig/lib/jyson-1.0.2.jar;
REGISTER '../udf_python/json_parse.py' USING jython AS converter;
REGISTER '../udf_python/euclidean.py' USING jython AS euclidean;

-- SCRIPT PARA OBTENER DATA DE UN ID EN ESPECIFICO

data = LOAD '/user/gtr/data/year=2014/month=08/day=*/hour=*/day*' AS line:CHARARRAY;

date_filter = FILTER data BY line MATCHES '.*\\"transmit\\":\\"\\d{4}-\\d{2}-\\d{2}.*';
content = FILTER date_filter BY line MATCHES '$regex';

tsv = FOREACH content GENERATE converter.tsvify(line);

four_fields = FOREACH tsv GENERATE FLATTEN($0);

final_data = FOREACH four_fields GENERATE latitud AS a:double, longitud AS b:double, ToDate(gps,'yyyy-MM-dd HH:mm:ss') AS c:datetime;

--DESCRIBE final_data;

STORE final_data INTO '/user/gtr/equipos_cluster/$regex';


