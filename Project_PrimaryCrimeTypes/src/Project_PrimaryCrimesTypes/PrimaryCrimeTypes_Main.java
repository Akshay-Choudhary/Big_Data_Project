/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_PrimaryCrimesTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author akshay
 */
public class PrimaryCrimeTypes_Main extends Configured implements Tool {
    
    
    public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException{
        try{
            int res = ToolRunner.run(new Configuration(), new PrimaryCrimeTypes_Main(), args);
        }catch (Exception e){
            
        }
        
    }
    
    public int run(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"domestic_true_false");
        job.setJarByClass(PrimaryCrimeTypes_Main.class);
        job.setMapperClass(PCT_Mapper.class);
        job.setReducerClass(PCT_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"SecondReducerJoin");
        
        
        if(complete){
        
        job2.setJarByClass(PrimaryCrimeTypes_Main.class);
        job2.setMapperClass(PCT_Mapper2.class);
        job2.setReducerClass(PCT_Reducer2.class);

        job2.setOutputKeyClass(PrimaryCrimeTypesWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        }
        
        boolean success = job2.waitForCompletion(true);
        return success ? 0 : 2;
    
    
    
    }
    
    public static class PCT_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        private IntWritable one = new IntWritable(1);
        
        public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException{
            String[] values = value.toString().split(",");
            String crime_type = values[5].trim();
            context.write(new Text(crime_type), one);
            
            
        }
        
    }
    
    public static class PCT_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
            int count=0;
            
            for(IntWritable val: values){
             count++;   
            }
            
            context.write(key, new IntWritable(count));
            
        }
        
    }
    
    public static class PCT_Mapper2 extends Mapper<Object, Text, PrimaryCrimeTypesWritable, NullWritable>{
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            
            String[] values = value.toString().split("\t");
            
            PrimaryCrimeTypesWritable pct = new PrimaryCrimeTypesWritable(values[0],Integer.parseInt(values[1]));
            context.write(pct, NullWritable.get());
            
        }
        
    }
    
    public static class PrimaryCrimeTypesWritable implements Writable, WritableComparable<PrimaryCrimeTypesWritable>{
        private String theft;
        private Integer count;
        
        public PrimaryCrimeTypesWritable(){
            
        }
        
        public PrimaryCrimeTypesWritable(String t,int c){
            this.theft = t;
            this.count = c;
        }
        
        
        @Override
        public void write(DataOutput d) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            WritableUtils.writeString(d, theft);
            WritableUtils.writeVInt(d, count);
        }

        public String getTheft() {
            return theft;
        }

        public void setTheft(String theft) {
            this.theft = theft;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            theft = WritableUtils.readString(di);
            count = WritableUtils.readVInt(di);
        }

        @Override
        public int compareTo(PrimaryCrimeTypesWritable o) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        int result = count.compareTo(o.count);
            if(result ==0){
                result = theft.compareTo(o.theft);
            }
            return -1 * result;
        }
        
        public String toString()
        {
        return (new StringBuilder().append(theft).append("\t").append(count).toString());
        }
        
    }
    
    public static class PCT_Reducer2 extends Reducer<Text, IntWritable, PrimaryCrimeTypesWritable, NullWritable>{
        
        public void reduce(PrimaryCrimeTypesWritable key, Iterable<NullWritable>values, Context context)throws IOException, InterruptedException{
            
            for(NullWritable val : values){
                context.write(key, NullWritable.get());
            }
        }
        
    }
}
