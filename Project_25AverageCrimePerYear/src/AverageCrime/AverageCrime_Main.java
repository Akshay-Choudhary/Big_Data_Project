/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AverageCrime;

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
public class AverageCrime_Main extends Configured implements Tool{
    
    
    public static void main(String[] args)throws IOException, InterruptedException,ClassNotFoundException{
        try{
            int res = ToolRunner.run(new Configuration(), new AverageCrime_Main(), args);
        }catch (Exception e){
            
        }
        
        
    }
    
    public int run(String[] args)throws Exception{
        
        Configuration conf= new Configuration();
        
      
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(AverageCrime_Main.class);
        job.setMapperClass(AverageCrime_Mapper.class);
        //job.setCombinerClass(AverageCrime_Reducer.class);
        job.setReducerClass(AverageCrime_Reducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
      
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        boolean success = job.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"SecondReducerJoin");

        if(success){
        job2.setJarByClass(AverageCrime_Main.class);
        
        job2.setMapperClass(AverageCrime_Mapper2.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(CompositeKeyWritable.class);
        
        job2.setReducerClass(AverageCrime_Reducer2.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        }
        boolean complete = job2.waitForCompletion(true);
        return complete ? 0 : 2;
        
    }

    
    public static class AverageCrime_Mapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text date_year = new Text();
        private IntWritable one = new IntWritable();
        //private CompositeKeyWritable cw = new CompositeKeyWritable();
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
        String[] values = value.toString().split(",");
        String date = values[2].trim();
        String month_date_year = date.substring(0,10);
        date_year.set(month_date_year);
        //cw.setCount(1);
        context.write(date_year, one);
        
        }
    }
    
    public static class AverageCrime_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        
        public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException{
            
            int count = 0;
            for(IntWritable val : values){
                count++;
            }
            
            context.write(key, new IntWritable(count));
        }
    }
    
    
    public static class AverageCrime_Mapper2 extends Mapper<Object, Text, NullWritable, CompositeKeyWritable>{
        
        private CompositeKeyWritable cw = new CompositeKeyWritable();
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            
            String[] values = value.toString().split("\t");
            String date_year = values[0].trim();
            int crime_count = Integer.parseInt(values[1].trim());
            cw.setCount(1);
            cw.setAvg_crime_count(crime_count);
            
            context.write(NullWritable.get(), cw);
            
        }
        
    }
    
    
    public static class AverageCrime_Reducer2 extends Reducer<NullWritable, CompositeKeyWritable, NullWritable, IntWritable>{
        
        private IntWritable avg_crime_per_day = new IntWritable();
        
        public void reduce(NullWritable key, Iterable<CompositeKeyWritable> values, Context context)throws IOException, InterruptedException{
            
            int sum = 0;
            int count=0;
            int avg = 0;
            
            for(CompositeKeyWritable val : values){
                sum+= val.getCount() * val.getAvg_crime_count();
                count+= val.getCount();
            }
            System.out.println(sum);
            System.out.println(count);
            
            
            avg = sum/count;
            System.out.println(avg);
            avg_crime_per_day.set(avg);
            context.write(NullWritable.get(), avg_crime_per_day);
        }
    }
    
    public static class CompositeKeyWritable implements Writable{
        
        private int count;
        private int avg_crime_count;

        
        
        public CompositeKeyWritable(){
            
        }
        

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public int getAvg_crime_count() {
            return avg_crime_count;
        }

        public void setAvg_crime_count(int avg_crime_count) {
            this.avg_crime_count = avg_crime_count;
        }
        
        
        @Override
        public void write(DataOutput d) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
           d.writeInt(count);
           d.writeInt(avg_crime_count);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            count = di.readInt();
            avg_crime_count = di.readInt();
        }
        
        public String toString(){
            return  count + "\t" + avg_crime_count;
        }
    
    }
    
}
    
    
    
