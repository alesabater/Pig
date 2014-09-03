REGISTER /usr/lib/pig/lib/Pighout-0.1-jar-with-dependencies.jar;

DEFINE udf_cluster sigis.pighout.CreatePaths();
DEFINE get_initial_clusters sigis.pighout.CanopyCluster();
DEFINE k_means sigis.pighout.KmeansClustering();

data = LOAD '/user/gtr/nuevo/pig_sample/*/' USING PigStorage(',') AS (id:chararray, date:chararray, lon:double, lat:double, interval:double, hour:double);

data_grouped_id = GROUP data BY id;

data_lon_lat_hour = FOREACH data_grouped_id{
			tuple1 = FOREACH data GENERATE lon,lat,hour;
			GENERATE group, tuple1;
			}


data_clusterized = FOREACH data_lon_lat_hour GENERATE group, FLATTEN(udf_cluster(tuple1, group));

STORE data_clusterized INTO '/user/gtr/cluster_paths/paths_by_id-1' USING PigStorage(',');

canopy_info = FOREACH data_clusterized GENERATE get_initial_clusters(TOTUPLE($1,$2,$3));

cluster_info = FOREACH data_clusterized GENERATE k_means(TOTUPLE($1,$2,$3));

DUMP canopy_info;

--DUMP data_clusterized;

--STORE data_clusterized INTO '/user/gtr/cluster_paths/paths_by_id-1' USING PigStorage(',');

--data_path = LOAD '/user/gtr/cluster_paths/paths_by_id-1/' USING PigStorage(',') AS (id:chararray, input_path:chararray, cluster_path:chararray, output_path:chararray);

--data_initial_clusters = FOREACH data_path GENERATE get_initial_clusters(TOTUPLE($1,$2,$3));

--data_initial_clusters = FOREACH data_clusterized GENERATE get_initial_clusters(TOTUPLE($1,$2,$3));




