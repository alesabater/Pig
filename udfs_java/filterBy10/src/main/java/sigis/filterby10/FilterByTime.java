package sigis.filterby10;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class FilterByTime extends EvalFunc<DataBag> {
    
    private final static Logger logger = LoggerFactory.getLogger(FilterByTime.class);
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();  
    long size;
    
        public DataBag exec(Tuple tuple) throws IOException {
        
        DataBag output = mBagFactory.newDefaultBag();
        double[] latlon = new double[2];
        DateTime date1 = null;
        DateTime date2 = null;
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        
        if (tuple == null) {
            throw new IllegalArgumentException("Ilegal parameter passed in");
        }
        
        try{
            DataBag columns = (DataBag)tuple.get(0);
            Object threshold = tuple.get(1);

            Integer count = 0;
            for(Tuple column : columns)
            {
                //column = (sgps1472,-64.6757666666667,10.1161833333333,2014-08-05 03:38:14)
                
                    String row = column.toString().substring(1, column.toString().length() - 1);
                    String[] values = row.split(",");

                   if (count==0){
                       date1 = formatter.parseDateTime(values[3]);
                       count++;
                   }
                   else{
                       //latlon2[0] = Double.parseDouble(values[1]);
                       date2 = formatter.parseDateTime(values[3]);
                       count = 0;
                   }

                   if(date1!=null && date2!=null){
                       DateTime biggerDate;
                       long millis;
                       if(date1.isAfter(date2)){
                           biggerDate = date1;
                           millis = date1.getMillis() - date2.getMillis();
                       } else {
                           biggerDate = date2;
                           millis = date2.getMillis() - date1.getMillis();
                       }
                       
                       double finalSome = millis*0.0001*0.16666;
                       
                       if (finalSome >= (Double)threshold){
                    	   	int hour = biggerDate.getHourOfDay();
                    	   	int minute = biggerDate.getMinuteOfHour();
                    	   	String hourConverted = hour + String.valueOf(minute/60.0000).substring(1);
                            latlon[0] = Double.parseDouble(values[1]);
                            latlon[1] = Double.parseDouble(values[2]);
                            Tuple report = mTupleFactory.newTuple(5);
                            report.set(0, biggerDate.toString());
                            report.set(1, latlon[0]);
                            report.set(2, latlon[1]);
                            report.set(3, finalSome);
                            report.set(4, hourConverted);
                            output.add(report);
                       }
                }
                
            }
            if (output.size()==0){
            	/*
                String nullValue = "null";
                Tuple report = mTupleFactory.newTuple(nullValue);
                output.add(report);*/
            	
                return output;
            }
            else{
                return output;
            }

        }
        catch (Exception e) {
            throw new IOException(tuple.get(0).getClass().toString()+ "Error parseando los datos", e);
        }
        
    }
        
    public Schema outputSchema(Schema input) {

       Schema bagSchema;
       bagSchema = new Schema();
       bagSchema.add(new Schema.FieldSchema("fecha", DataType.CHARARRAY));
       bagSchema.add(new Schema.FieldSchema("lon", DataType.DOUBLE));
       bagSchema.add(new Schema.FieldSchema("lat", DataType.DOUBLE));
       bagSchema.add(new Schema.FieldSchema("tiempoEntreReporte", DataType.DOUBLE));
       bagSchema.add(new Schema.FieldSchema("hora", DataType.DOUBLE));
       String schemaName =  "points";
       
        try {
            return new Schema(new Schema.FieldSchema(schemaName,bagSchema,DataType.BAG));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

/*
    @Override
    public Schema outputSchema(Schema input)
    {
        return input;
    }*/


}