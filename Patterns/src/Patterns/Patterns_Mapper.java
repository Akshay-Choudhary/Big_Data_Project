/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author akshay
 */
public class Patterns_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private Text domestic_incident = new Text();
    private IntWritable one = new IntWritable(1);
    
    public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException{
        String header = value.toString();
        if(header.contains("Domestic")){
           return;
        }else{
        String[] values = value.toString().split(",");
        //System.out.println(values[0] + "-" + values[1] + "-" + values[2] + "-" + values[3] + "- " + values[4] + "-" + values[5] + "-" + values[6] + "-" + values[7] +"####" + values[8] + "#################" + values[9]);
        String domestic = values[9];
        domestic_incident.set(domestic);
        context.write(domestic_incident, one);
        } 
    }
}
