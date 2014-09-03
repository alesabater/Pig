/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.clustering.iterator.ClusterWritable;

/**
 *
 * @author asabater
 */
public class ReaderSeqFile {
    
    public static void main( String[] args ) throws IOException, URISyntaxException{
       
            //URI uri = new URI("/home/asabater/data_to_cluster");
            URI uri = new URI("/home/asabater/Desktop/part-r-00000");
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(uri, conf);
            Path path = new Path(uri);
            FileOutputStream out = new FileOutputStream("/home/asabater/nuevo/cluster");
            SequenceFile.Reader reader = null;
            try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable)
            ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            ClusterWritable value = (ClusterWritable)
            //final WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
            ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
                System.out.println("TRYING:" + reader.getPosition());
            while (reader.next(key, value)) {
                System.out.println(value.getClass());
                System.out.printf("%s\t%s\n",key, value.getValue().toString());
                position = reader.getPosition(); // beginning of next record
            }
            out.close();
            } finally {
            IOUtils.closeStream(reader);
            }
   }
    
}
