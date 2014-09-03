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
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.slf4j.LoggerFactory;

/**
 *
 * @author asabater
 */
public class CanopyCluster extends EvalFunc<String> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    private final static org.slf4j.Logger logger = LoggerFactory.getLogger(CanopyCluster.class);
    
    @Override
    public String exec(Tuple tuple) throws IOException{
      
        final Configuration conf = UDFContext.getUDFContext().getJobConf();
        FileSystem fs = FileSystem.get(conf);
        
        try{
            Tuple POINTS_PATH = (Tuple)tuple.get(0);
            String CLUSTER_PATH = (String)tuple.get(1);
            //riteInitialCentroidsCanopy(conf, POINTS_PATH, CLUSTER_PATH);
            logger.info("Canopy clusters finished");
            return CLUSTER_PATH;
        }
        
        catch (ExecException ex){
            Logger.getLogger(CanopyCluster.class.getName()).log(Level.SEVERE, null, ex);  
        }/* catch (InterruptedException ex) {
            Logger.getLogger(CanopyCluster.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(CanopyCluster.class.getName()).log(Level.SEVERE, null, ex);
        }*/
        return "TRUE";
    }
    
    private void writeInitialCentroidsCanopy(final Configuration conf, String POINTS_PATH, String CLUSTER_PATH) throws IOException, InterruptedException, ClassNotFoundException{
        
        CanopyDriver.run(new Path(POINTS_PATH), new Path(CLUSTER_PATH),
                         new EuclideanDistanceMeasure(),(float) 3.1, (float) 2.1, false, 0.5, false);
        
        System.out.println(Cluster.FINAL_ITERATION_SUFFIX);
        System.out.println(Cluster.CLUSTERED_POINTS_DIR);
    }
    
}
