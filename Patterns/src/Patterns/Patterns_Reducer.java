/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author akshay
 */
public class Patterns_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private Text true_false = new Text();
    private IntWritable count = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
        int sum = 0;
        
        for(IntWritable val: values){
            sum+= val.get();
        }
        count.set(sum);
        true_false.set(key);
        context.write(true_false, count);
    }
}
