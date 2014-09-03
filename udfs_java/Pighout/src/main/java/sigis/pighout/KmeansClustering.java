/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package sigis.pighout;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

/**
 *
 * @author asabater
 */
public class KmeansClustering extends EvalFunc<String> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    private final String CLUSTER_EXT = "/clusters-0-final/part-r-00000";
    
    @Override
    public String exec(Tuple tuple) throws IOException{
      
        final Configuration conf = UDFContext.getUDFContext().getJobConf();
        FileSystem fs = FileSystem.get(conf);
        
        try{
            String POINTS_PATH = (String)tuple.get(0);
            String CLUSTER_PATH = (String)tuple.get(1);
            String OUTPUT_PATH = (String)tuple.get(2);
            performKmeans(conf, POINTS_PATH, CLUSTER_PATH, OUTPUT_PATH);
        }
        
        catch (ExecException ex){
            Logger.getLogger(CanopyCluster.class.getName()).log(Level.SEVERE, null, ex);  
        } catch (InterruptedException ex) {
            Logger.getLogger(CanopyCluster.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(CanopyCluster.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "Initial Clusters created";
    }
    
    private void performKmeans(final Configuration conf, String POINTS_PATH, String CLUSTER_PATH, String OUTPUT_PATH) throws IOException, InterruptedException, ClassNotFoundException{
        
        KMeansDriver.run(conf,new Path(POINTS_PATH), new Path(CLUSTER_PATH + CLUSTER_EXT), new Path(OUTPUT_PATH), 0.001, 10, true, 0, false);
    }
    
}