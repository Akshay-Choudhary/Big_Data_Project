/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Yearly_Districtwise_Crime;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author akshay
 */
public class Yearly_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    public IntWritable dist_count = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
        int count = 0;
        for(IntWritable val : values){
            count++;
        }
        dist_count.set(count);
        context.write(key, dist_count);
    }
    
}

