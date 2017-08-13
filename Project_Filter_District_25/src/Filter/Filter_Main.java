/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Filter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akshay
 */
public class Filter_Main {
    
    
    public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException{
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"domestic_true_false");
        job.setJarByClass(Filter_Main.class);
        job.setMapperClass(Filter_Mapper.class);
        job.setNumReduceTasks(0);
        job.getConfiguration().set("community_area_code", "25.0");
        //job.setReducerClass(Filter_Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        
        
        
    }
    
    
    public static class Filter_Mapper extends Mapper<Object, Text, NullWritable, Text>{
        
        private String community_area = null;
        
        public void setup(Context context)throws IOException, InterruptedException{
            community_area = context.getConfiguration().get("community_area_code");
        }
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            
            String[] values= value.toString().split(",");
            String area_code = values[13].trim();
            System.out.println(area_code);
            if(area_code.contains(community_area)){
                context.write(NullWritable.get(), value);
            }
            else
                return;
        }
        
    }
    
}
