package sigis.pighout;


// Common imports
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

// Hadoop imports

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.canopy.CanopyDriver;

// Mahout imports
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

// Pig imports
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;


public class CreatePaths extends EvalFunc<Tuple> {
    
    private final String INITIAL_PATH;
    private String POINTS_PATH;
    private String CLUSTERS_PATH;
    private String OUTPUT_PATH;
    private String ID_PATH;
    private String idGps;
    
    private final static Logger logger = LoggerFactory.getLogger(CreatePaths.class);
    
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    public CreatePaths(String param){
        this.INITIAL_PATH = param;
        logger.info("Using user defined path");
    }
    
    public CreatePaths(){
        this.INITIAL_PATH = "/user/gtr/anonymous";
        logger.info("Using default path");
    }

    public Tuple exec(Tuple tuple) throws IOException {
        
        //Gets the job context. That means variables attributes and more
        final Configuration conf = UDFContext.getUDFContext().getJobConf();
        FileSystem fs = FileSystem.get(conf);
        
        // Creating tuple of size 3 to be returned
        Tuple report = mTupleFactory.newTuple(3);
        
        //DenseVector to compute Kmeans
        List<DenseVector> positions = new ArrayList<DenseVector>();
        
        if (tuple == null) {
            logger.error("Empty tuple");
            throw new IllegalArgumentException("Empty input tuple");         
        }
 
        try{
            //Reading Input data from pig
            DataBag rows = (DataBag)tuple.get(0);
            this.idGps = (String)tuple.get(1);
            
            // Get every value of an instance row and save it to a double array for later creation of a Vector
            for(Tuple register :rows){
                      
                double[] features = fromTupleToArray(register);
                DenseVector position = new DenseVector(features);
                positions.add(position);
                
            }
            Path path = new Path(INITIAL_PATH);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            createDirectories(fs);
            createInputPath(conf, fs, positions); 

            /*
            try {
                
                clusterizationLoop(positions, conf , fs);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(CreatePaths.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                java.util.logging.Logger.getLogger(CreatePaths.class.getName()).log(Level.SEVERE, null, ex);
            }*/
            report.set(0, POINTS_PATH);
            report.set(1, CLUSTERS_PATH);
            report.set(2, OUTPUT_PATH);
            return report;

        }
        catch (ExecException e){
             throw new IOException(tuple.getClass().toString()+ "<--- Class of the received input", e);
        }
        
    }
       
    // Method to go through values of a instance row and put them into a double[]
    private double[] fromTupleToArray(Tuple T) throws ExecException{
        
        double[] features = new double[T.size()];
        
        for(int i = 0; i<T.size(); i++){
            features[i] = (Double)T.get(i);
        }
        
        return features;    
    }
        
    //Method in charge of calling Kmeans clustering for each different K
    private void clusterizationLoop(List<DenseVector> positions, Configuration conf ,FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException{
         
        
        
        Path path = new Path(INITIAL_PATH);
        
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        
        createDirectories(fs);
        createInputPath(conf, fs, positions); 
        //writeInitialCentroidsCanopy(conf);
        }
        //for(int k = this.Kmin; k<= this.Kmax; k++){
            
            
            //writeClusterInitialCenters(conf, fs, positions, k);
            
            //fs.mkdirs(new Path(this.OUTPUT_PATH + "/K=" + k));
            /*
            KMeansDriver.run( 
                             new Path(this.POINTS_PATH+ "/pointsFile"),
                             new Path(this.CLUSTERS_PATH + "/clusters-0-final/part-r-00000"),
                             new Path(this.OUTPUT_PATH),      
                             0.001,
                             10,
                             true,
                             0,
                             false);*/
        //}
        
    
    
    // Creates output, clusters, points directories. It also creates a directory with idGps on top of the others
    private void createDirectories(FileSystem fs) throws IOException{
        
        this.ID_PATH = this.INITIAL_PATH + "/" + this.idGps;
        this.POINTS_PATH = ID_PATH + "/points";
        this.CLUSTERS_PATH = ID_PATH + "/clusters";
        this.OUTPUT_PATH = ID_PATH + "/output";
        
        fs.mkdirs(new Path(ID_PATH));
        fs.mkdirs(new Path(POINTS_PATH));
        fs.mkdirs(new Path(CLUSTERS_PATH));
        fs.mkdirs(new Path(OUTPUT_PATH));  
        
        logger.info("Directories created");
    }
    
    // Save points to clusterize into a directory called points/ and a file called pointsFile
    private void createInputPath(Configuration conf, FileSystem fs,List<DenseVector> positions) throws IOException{
        
        this.POINTS_PATH = this.POINTS_PATH + "/pointsFile";
        
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, new Path(this.POINTS_PATH), 
                                                             Text.class,
                                                             VectorWritable.class);
        VectorWritable vec = new VectorWritable();
        for (Integer i=0; i<positions.size();i++){
            vec.set(positions.get(i));
            writer.append(new Text(i.toString()), vec);
        }
	writer.close();
        
    }
    
    /*
    private void writeInitialCentroidsCanopy(final Configuration conf) throws IOException, InterruptedException, ClassNotFoundException{
        
        CanopyDriver.run(new Path(this.POINTS_PATH), new Path(this.CLUSTERS_PATH),
                         new EuclideanDistanceMeasure(),(float) 3.1, (float) 2.1, false, 0.5, false);
    }*/
    
    // Write to a file clusters/K=k__part-00000 the initial clusters
    /*
    private void writeClusterInitialCenters(final Configuration conf, FileSystem fs, List<DenseVector> positions, int K) throws IOException {
        
        //final Path writerPath = new Path(this.CLUSTERS_PATH + "/K="+K+"_part-00000");
        String iterationCluster = CLUSTERS_PATH+"/K="+K;
        fs.mkdirs(new Path(iterationCluster));
        
        
        final SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                                           SequenceFile.Writer.file(new Path(iterationCluster+"/part-00000")),
                                           SequenceFile.Writer.keyClass(Text.class),
                                           SequenceFile.Writer.valueClass(Kluster.class));
        
        Random rand = new Random();
        for (int i = 0; i < K; i++) {
            int randomNum = rand.nextInt((positions.size() - 0));
            final Vector vec = positions.get(randomNum);
            final Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();
    }
    
    private void writeClusterInitialCenters(final Configuration conf, FileSystem fs, List<DenseVector> positions, int K) throws IOException {
        
        //this.CLUSTERS_PATH = this.CLUSTERS_PATH + "/K="+K+"_part-00000";
        //final Path writerPath = new Path(this.CLUSTERS_PATH);
        
        final Path writerPath = new Path(this.CLUSTERS_PATH + "/K="+K+"_part-00000");
        
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, writerPath, 
                                                             Text.class,
                                                             VectorWritable.class);
        VectorWritable vec = new VectorWritable();
        Random rand = new Random();
        for (int i = 0; i < K; i++) {
            Integer randomNum = rand.nextInt((positions.size() - 0));
            vec.set(positions.get(randomNum));
            //final Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            warn(positions.get(randomNum).toString(),  PigWarning.UDF_WARNING_1);
            writer.append(new Text(randomNum.toString()), vec);
        }
        writer.close();
    }
    */
        
    @Override
    public Schema outputSchema(Schema input) {

       Schema tupleSchema;
       tupleSchema = new Schema();
       tupleSchema.add(new Schema.FieldSchema("points", DataType.CHARARRAY));
       tupleSchema.add(new Schema.FieldSchema("cluster", DataType.CHARARRAY));
       tupleSchema.add(new Schema.FieldSchema("output", DataType.CHARARRAY));
             
        try {
            return new Schema(new Schema.FieldSchema("paths",tupleSchema,DataType.TUPLE));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
