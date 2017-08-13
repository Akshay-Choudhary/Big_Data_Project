/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Binning;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author akshay
 */
public class Binning_Mapper extends Mapper<Object, Text, Text, NullWritable>{
    private MultipleOutputs<Text, NullWritable> mos = null;
    
    protected void setup(Mapper.Context context){
        mos = new MultipleOutputs(context);
    }
    
    public void map(Object key, Text value, Mapper.Context context) throws IOException,InterruptedException{
        String records[] = value.toString().split(",");
        String year = records[17].trim();
        if(year.equalsIgnoreCase("2008"))
            mos.write("bins",value, NullWritable.get(), "2008");    
        if(year.equalsIgnoreCase("2009"))
            mos.write("bins",value, NullWritable.get(), "2009");
        if(year.equalsIgnoreCase("2010"))
            mos.write("bins",value, NullWritable.get(), "2010");
        if(year.equalsIgnoreCase("2011"))
            mos.write("bins",value, NullWritable.get(), "2011");
        if(year.equalsIgnoreCase("2012"))
            mos.write("bins",value, NullWritable.get(), "2012");
        if(year.equalsIgnoreCase("2013"))
            mos.write("bins",value, NullWritable.get(), "2013");
        if(year.equalsIgnoreCase("2014"))
            mos.write("bins",value, NullWritable.get(), "2014");
        if(year.equalsIgnoreCase("2015"))
            mos.write("bins",value, NullWritable.get(), "2015");
        if(year.equalsIgnoreCase("2016"))
            mos.write("bins",value, NullWritable.get(), "2016");
        
    }
        
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException{
        mos.close();
    }
}
