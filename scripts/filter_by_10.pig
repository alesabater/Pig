REGISTER /usr/lib/pig/lib/jyson-1.0.2.jar;
REGISTER '../udf_python/json_parse.py' USING jython AS converter;
REGISTER ../udf_java/filterBy10-0.1.jar;
REGISTER piggybank.jar;
REGISTER commons-math3-3.3.jar;

DEFINE filterby10 sigis.filterby10.FilterByTime();
DEFINE MultiStorage org.apache.pig.piggybank.storage.MultiStorage('/user/gtr/FilteredByTime', '0', 'none',',');

-- SCRIPT PARA DESCARTAR EVENTOS PERTENECIENTES A TRAYECTORIAS

data = LOAD '/user/gtr/data/year=2014/month=$month/day=$day/hour=10/day*' AS line:CHARARRAY;


-- FILTRAR POR LOS CAMPOS TRANSMIT, LATITUD, LONGITUD.
data_filter = FILTER data BY line MATCHES '.*\\"lon\\":-?\\d{1,2}.\\d+.*\\"lat\\":-?\\d{1,2}.\\d+.*\\"transmit\\":\\"\\d{4}-\\d{2}-\\d{2}.*';

shortened_data = FOREACH data_filter GENERATE converter.tsvify(line);

flatten_data = FOREACH shortened_data GENERATE FLATTEN($0);

grouped_data = GROUP flatten_data by id_equipo;

grouped_by_id = FOREACH grouped_data{
                        ordered_data = ORDER flatten_data BY gps;
                        GENERATE group, filterby10(ordered_data,10.00);
                        --GENERATE group, ordered_data;
                        }

--geoinfo_no_nulls = FILTER geoinfo BY (geomap#'geoLocation' is not null);

--parsed_no_nulls = FILTER grouped_by_id BY points.fecha IS NOT NULL;

--excedidos = FILTER grouped_by_id BY points.fecha MATCHES '^\\d{4}.*';

STORE grouped_by_id INTO '/user/gtr/FilteredByTime' USING MultiStorage('user/gtr/FilteredByTime', '0');

