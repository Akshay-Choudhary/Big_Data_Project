/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Yearly_Districtwise_Crime;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author akshay
 */
public class Yearly_Main{
   
    
    
    public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        String[] names ={"2008-m-00000","2008-m-00001","2009-m-00000","2009-m-00001","2010-m-00000","2011-m-00000","2012-m-00001","2012-m-00002","2013-m-00001","2013-m-00002","2014-m-00001","2014-m-00002","2015-m-00001","2016-m-00001"};
        String[] outNames = {"2008Output", "2008Output1", "2009Output","2009Output1","2010Output","2011Output","2012Output1","2012Output2","2013Output1","2013Output2","2014Output1","2014Output2","2015Output1","2016Output1"};
    
//        Path inPath = new Path(args[0]);
//        Path outPath = new Path(args[1]);
        int c = 0;
//        String fileName = inPath.getName() + "/2008-m-00000" ;
        
        for(int i=0; i<14; i++){
        
        //Path in = new Path(fileName);
        String hdfsPath="hdfs://localhost:9000/Analysis2/AnalysisOutput/Output/";
        String outPath = "hdfs://localhost:9000/Analysis3/Output/";
        String fileName = hdfsPath + names[i];
        String outFileName = outPath + outNames[i];
        System.out.println(fileName);
        
        Job job = Job.getInstance(conf, "Yearly_Districtwise_Count");
        job.setJarByClass(Yearly_Main.class);
        job.setJobName("Group By Year");
        job.setMapperClass(Yearly_Mapper.class);
        job.setReducerClass(Yearly_Reducer.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, IntWritable.class);
        
        //MultipleOutputs.setCountersEnabled(job, true);
        
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(fileName));
        FileOutputFormat.setOutputPath(job, new Path(outFileName));
        //FileInputFormat.setInputPaths(job, in);
        //FileOutputFormat.setOutputPath(job, outPath);
        c++;
        //job.waitForCompletion(true);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        
        if(i==14){
            System.exit(status);
        }
        
        }
        
    }

}
