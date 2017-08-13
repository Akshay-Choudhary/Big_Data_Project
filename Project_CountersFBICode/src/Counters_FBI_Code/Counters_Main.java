/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Counters_FBI_Code;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akshay
 */
public class Counters_Main {
    
    
    public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf= new Configuration();
        
      
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(Counters_Main.class);
        job.setMapperClass(Counters_Mapper.class);
        //job.setCombinerClass(AverageCrime_Reducer.class);
        //job.setReducerClass(AverageCrime_Reducer.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
      
        //FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        int code = job.waitForCompletion(true) ? 0 : 1;
        
        if(code== 0){
            for(Counter counter : job.getCounters().getGroup(Counters_Mapper.FBI_COUNTER_GROUP)){
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }
        
        FileSystem.get(conf).delete(new Path(args[1]), true);
        
        System.exit(code);
    }
    
    
    public static class Counters_Mapper extends Mapper<Object, Text, NullWritable, NullWritable>{
        
        public static final String FBI_COUNTER_GROUP = "FBI_CODE";
        public static final String UNKNOWN_COUNTER = "Unknown";
        public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";
        
        private String[] codeArray = new String[] {"08B","24","06","04B","15","03","07","08A","26","11","14","05","02","18","17","04A","20","01A","10","16","19","13","09","22","12","01B"};
        
        private HashSet<String> codes = new HashSet<String>(Arrays.asList(codeArray));
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            
            String[] values = value.toString().split(",");
            String fbi_code = values[14].trim();
            //System.out.println(fbi_code);
            
            boolean unknown = true;
            if(codes.contains(fbi_code)){
                
                context.getCounter(FBI_COUNTER_GROUP, fbi_code).increment(1);
                unknown = false;
               
            }
            
            if(unknown){
                context.getCounter(FBI_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
            }
       
            
        }
        
        
    }
    
}
