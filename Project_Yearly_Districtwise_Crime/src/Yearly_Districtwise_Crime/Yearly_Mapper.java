/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Yearly_Districtwise_Crime;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author akshay
 */
public class Yearly_Mapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private Text district = new Text();
    private IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String values[] = value.toString().split(",");
        String distric_code = values[11].trim();
        district.set(distric_code);
        context.write(district, one);
    }
}
