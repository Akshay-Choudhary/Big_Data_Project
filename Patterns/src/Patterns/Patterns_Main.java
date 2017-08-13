/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akshay
 */
public class Patterns_Main {
    public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);   
        
        Path inputDir = new Path(args[0]);
        Path hdfsfile = new Path(args[1]);
        
        try{
            FileStatus[] inputfiles = local.listStatus(inputDir);
            FSDataOutputStream out = hdfs.create(hdfsfile);
            
            for(int i=0; i<inputfiles.length; i++){
                FSDataInputStream in = local.open(inputfiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesread = 0;
                while((bytesread = in.read(buffer)) > 0){
                    out.write(buffer,0,bytesread);
                }
                in.close();
            }
            out.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
        
        Job job = Job.getInstance(conf,"domestic_true_false");
        job.setJarByClass(Patterns_Main.class);
        job.setMapperClass(Patterns_Mapper.class);
        job.setReducerClass(Patterns_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, hdfsfile);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
    
}
