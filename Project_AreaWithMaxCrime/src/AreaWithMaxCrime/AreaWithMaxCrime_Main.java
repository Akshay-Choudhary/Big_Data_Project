/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AreaWithMaxCrime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class AreaWithMaxCrime_Main extends Configured implements Tool {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        try{
            int res = ToolRunner.run(new Configuration(), new AreaWithMaxCrime_Main(), args);
        }catch (Exception e){
            
        }
    }
        
        
        public int run(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"domestic_true_false");
        job.setJarByClass(AreaWithMaxCrime_Main.class);
        job.setMapperClass(AreaWithMaxCrime_Mapper.class);
        job.setReducerClass(AreaWithMaxCrime_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"SecondReducerJoin");

        if(success){
        job2.setJarByClass(AreaWithMaxCrime_Main.class);
        job2.setMapperClass(AreaWithMaxCrime_Mapper2.class);
        job2.setReducerClass(AreaWithMaxCrime_Reducer2.class);

        job2.setOutputKeyClass(CompositeKeyWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        }
        boolean complete = job2.waitForCompletion(true);
        return complete ? 0 : 2;
        
        
    }
    
    
    public static class AreaWithMaxCrime_Mapper extends Mapper<Object, Text, Text, IntWritable>{
        
        private IntWritable one = new IntWritable();
        private Text area = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(",");
            String area_code = values[13].trim();
            area.set(area_code);
            context.write(area, one);
            
        }
        
    }
    
    public static class AreaWithMaxCrime_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
            
            int count = 0;
            for(IntWritable val : values){
                count++;
            }
            
            context.write(key, new IntWritable(count));
        }
        
    }
    
    
    public static class AreaWithMaxCrime_Mapper2 extends Mapper<Object, Text, CompositeKeyWritable, NullWritable>{
        private Text area1 = new Text();
        private IntWritable count = new IntWritable();
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
//            String[] values = value.toString().split("\t");
//            int crime_count = (Integer.parseInt(values[1].trim()));
//            month1.set(values[0]);
//            context.write(new IntWritable(crime_count), month1);
              String[] values = value.toString().split("\t");
          
              CompositeKeyWritable cw = new CompositeKeyWritable(values[0], Integer.parseInt(values[1]));
              context.write(cw, NullWritable.get());
              
        }
    }
    
    public static class AreaWithMaxCrime_Reducer2 extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable>{
        
        
        public void reduce(CompositeKeyWritable key, Iterable<NullWritable>values, Reducer.Context context)throws IOException, InterruptedException{
            
            for(NullWritable val : values){
                context.write(key, NullWritable.get());
            }
        }
    }
    
    
    
    public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

        private String area1;
        private Integer crime_count;
        
        public CompositeKeyWritable(){
            
        }
        
        public CompositeKeyWritable(String m, int c){
            this.area1 = m;
            this.crime_count = c;
        }

        public String getArea1() {
            return area1;
        }

        public void setArea1(String area1) {
            this.area1 = area1;
        }

        

        public int getCrime_count() {
            return crime_count;
        }

        public void setCrime_count(int crime_count) {
            this.crime_count = crime_count;
        }

        @Override
        public void write(DataOutput d) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            
            WritableUtils.writeString(d, area1);
            WritableUtils.writeVInt(d, crime_count);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            area1 = WritableUtils.readString(di);
            crime_count = WritableUtils.readVInt(di);
        }

        @Override
        public int compareTo(CompositeKeyWritable o) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            int result = crime_count.compareTo(o.crime_count);
            if(result ==0){
                result = area1.compareTo(o.area1);
            }
            return -1 * result;
        }
        public String toString()
        {
        return (new StringBuilder().append(area1).append("\t").append(crime_count).toString());
        }
        
    }
    
}
