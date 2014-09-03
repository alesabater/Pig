/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import java.io.BufferedReader;
import java.io.FileReader;
import static org.apache.commons.math3.util.Precision.round;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author asabater
 */
public class FileToSeqFile {
    
    	public static final int NUM_COLUMNS = 3;

	public  void createFile() throws Exception 
	{

		String INPUT_FILE =  "/home/asabater/Downloads/lat_lon_time.tsv";
		String OUTPUT_FILE = "/home/asabater/data_to_cluster";
                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                DateTime dateField = new DateTime();
		List<NamedVector> positions = new ArrayList<NamedVector>();
		NamedVector position;
		BufferedReader br = null;
		br = new BufferedReader(new FileReader(INPUT_FILE));
		String sCurrentLine;
                int count = 0;
		while ((sCurrentLine = br.readLine()) != null) {
                        
			String item_number = ""+count;
			double[] features = new double[NUM_COLUMNS];
			for(int indx=0; indx<NUM_COLUMNS;indx++){
                                if (indx == 2){
                                    double minute = round(Float.parseFloat(sCurrentLine.split("\\t")[indx].substring(14,16))/60,2);
                                    String min = "" + minute;
                                    min = min.substring(2);
                                    String hour = sCurrentLine.split("\\t")[indx].substring(11,13);
                                    String finalTime = hour+"."+min;
                                    features[indx] = round(Float.parseFloat(finalTime),2);
                                    
                                }
                                else{
                                    features[indx] = Float.parseFloat(sCurrentLine.split("\\t")[indx]);
                                }
			}

			position = new NamedVector(new DenseVector(features), item_number );
			positions.add(position);
                        count++;
	}

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(OUTPUT_FILE);

	SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, path, Text.class, VectorWritable.class);

	VectorWritable vec = new VectorWritable();
	for(NamedVector vector : positions){
	vec.set(vector);
	writer.append(new Text(vector.getName()), vec);
	}
	writer.close();

	SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT_FILE), conf);

	Text key = new Text();
	VectorWritable value = new VectorWritable();
	while(reader.next(key, value)){
		System.out.println(key.toString() + " , " + value.get().asFormatString());
	}
		reader.close();	
	}

    
}
