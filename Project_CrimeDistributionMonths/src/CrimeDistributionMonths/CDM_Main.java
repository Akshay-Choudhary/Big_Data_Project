/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CrimeDistributionMonths;

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
public class CDM_Main extends Configured implements Tool{
    
    
    public static void main(String[] args){
        try{
            int res = ToolRunner.run(new Configuration(), new CDM_Main(), args);
        }catch (Exception e){
            
        }
    }
        
        public int run(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"domestic_true_false");
            job.setJarByClass(CDM_Main.class);
            job.setMapperClass(CDM_Mapper.class);
            job.setReducerClass(CDM_Reducer.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            boolean success = job.waitForCompletion(true);
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2,"SecondReducerJoin");
            
            if(success){
            job2.setJarByClass(CDM_Main.class);
            job2.setMapperClass(CDM_Mapper2.class);
            job2.setReducerClass(CDM_Reducer2.class);
            
            job2.setOutputKeyClass(CompositeKeyWritable.class);
            job2.setOutputValueClass(NullWritable.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            
                
            }
            boolean complete = job2.waitForCompletion(true);
            return complete ? 0 : 2;
    }

    
    public static class CDM_Mapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text month = new Text();
        private IntWritable one = new IntWritable(1);
        
        public void map(Object key, Text value, Context context)throws IOException,InterruptedException{
            String values[] = value.toString().split(",");
            String date = values[2].trim();
            String month_number = date.substring(0,2);
            if(month_number.equalsIgnoreCase("01"))
                month.set("Jan");
            else if(month_number.equalsIgnoreCase("02"))
                month.set("Feb");
            else if(month_number.equalsIgnoreCase("03"))
                month.set("Mar");
            else if(month_number.equalsIgnoreCase("04"))
                month.set("Apr");
            else if(month_number.equalsIgnoreCase("05"))
                month.set("May");
            else if(month_number.equalsIgnoreCase("06"))
                month.set("Jun");
            else if(month_number.equalsIgnoreCase("07"))
                month.set("Jul");
            else if(month_number.equalsIgnoreCase("08"))
                month.set("Aug");
            else if(month_number.equalsIgnoreCase("09"))
                month.set("Sept");
            else if(month_number.equalsIgnoreCase("10"))
                month.set("Oct");
            else if(month_number.equalsIgnoreCase("11"))
                month.set("Nov");
            else if(month_number.equalsIgnoreCase("12"))
                month.set("Dec");
            
            context.write(month, one);
            
            
        }
    }   
    
    public static class CDM_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable months_count = new IntWritable();
        private Text peak_months = new Text();
        
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        
        for(IntWritable val: values){
            sum+= val.get();
        }
        months_count.set(sum);
        peak_months.set(key);
        context.write(peak_months, months_count);
        }
    }
    
    
    public static class CDM_Mapper2 extends Mapper<Object, Text, CompositeKeyWritable, NullWritable>{
        private Text month1 = new Text();
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
    
    public static class CDM_Reducer2 extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable>{
        
        
        public void reduce(CompositeKeyWritable key, Iterable<NullWritable>values, Context context)throws IOException, InterruptedException{
            
            for(NullWritable val : values){
                context.write(key, NullWritable.get());
            }
        }
    }
    
    public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

        private String month1;
        private Integer crime_count;
        
        public CompositeKeyWritable(){
            
        }
        
        public CompositeKeyWritable(String m, int c){
            this.month1 = m;
            this.crime_count = c;
        }

        public String getMonth1() {
            return month1;
        }

        public void setMonth1(String month1) {
            this.month1 = month1;
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
            
            WritableUtils.writeString(d, month1);
            WritableUtils.writeVInt(d, crime_count);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            month1 = WritableUtils.readString(di);
            crime_count = WritableUtils.readVInt(di);
        }

        @Override
        public int compareTo(CompositeKeyWritable o) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            int result = crime_count.compareTo(o.crime_count);
            if(result ==0){
                result = month1.compareTo(o.month1);
            }
            return -1 * result;
        }
        public String toString()
        {
        return (new StringBuilder().append(month1).append("\t").append(crime_count).toString());
        }
        
    }
}
