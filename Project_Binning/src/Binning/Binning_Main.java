/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Binning;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author akshay
 */
public class Binning_Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf,"Grouping");
		job.setJarByClass(Binning_Main.class);
		job.setJobName("Group By Year");
                //job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(NullWritable.class);
		job.setMapperClass(Binning_Mapper.class);
                //job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(NullWritable.class);
                
                MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
                
                MultipleOutputs.setCountersEnabled(job, true);
                
                //job.setNumReduceTasks(0);
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
