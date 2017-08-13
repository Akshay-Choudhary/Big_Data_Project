/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_ArrestRate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akshay
 */
public class ArrestRate_Main {
    
    
    public static void main(String[] args)throws IOException,InterruptedException,ClassNotFoundException{
        Configuration conf= new Configuration();
        
      
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(ArrestRate_Main.class);
        job.setMapperClass(ArrestRate_Mapper.class);
        job.setReducerClass(ArrestRate_Reducer.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ArrestRate_True_False.class);
      
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 :1);
    }
    
    public static class ArrestRate_Mapper extends Mapper<LongWritable, Text, NullWritable, ArrestRate_True_False>{
        
        private Text arrest = new Text();
        private ArrestRate_True_False atf = new ArrestRate_True_False();
        
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            String a = value.toString();
            if(a.contains("Arrest")){
                return;
            }
            
            
            String[] values = value.toString().split(",");
            String arrest_type = values[8].trim();
            if(arrest_type.equals("True")){
                //arrest.set(arrest_type);
                atf.setTrue_count(1);
                atf.setFalse_count(0);
                //System.out.println("Im True");
                atf.setPercentTrue(0);
            }else{
                //arrest.set(arrest_type);
                atf.setFalse_count(1);
                atf.setTrue_count(0);
                //System.out.println("Im False");
                atf.setPercentFalse(0);
            }
//            System.out.println(arrest_type);
//            System.out.println(atf.getTrue_count());
//            System.out.println(atf.getFalse_count());
//         
            context.write(NullWritable.get(), atf);
            
            
            
        }
    
    }
    
    
    public static class ArrestRate_True_False implements Writable {
   
        private int true_count;
        private int false_count;
        private float percentTrue;
        private float percentFalse;

        public float getPercentTrue() {
            return percentTrue;
        }

        public void setPercentTrue(float percentTrue) {
            this.percentTrue = percentTrue;
        }

        public float getPercentFalse() {
            return percentFalse;
        }

        public void setPercentFalse(float percentFalse) {
            this.percentFalse = percentFalse;
        }

        

        public int getTrue_count() {
            return true_count;
        }

        public void setTrue_count(int true_count) {
            this.true_count = true_count;
        }

        public int getFalse_count() {
            return false_count;
        }

        public void setFalse_count(int false_count) {
            this.false_count = false_count;
        }
        
        
        
        @Override
        public void write(DataOutput d) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            d.writeInt(true_count);
            d.writeInt(false_count);
            d.writeFloat(percentTrue);
            d.writeFloat(percentFalse);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            true_count = di.readInt();
            false_count = di.readInt();
            percentTrue = di.readFloat();
            percentFalse = di.readFloat();
        }
        
        public String toString(){
            return "True" + "\t" + true_count + "\t" + percentTrue + "\n" + "False" + "\t" + false_count + "\t" + percentFalse;
        }
        
    }
    
    
    public static class ArrestRate_Reducer extends Reducer<NullWritable, ArrestRate_True_False, NullWritable, ArrestRate_True_False>{
        
        private ArrestRate_True_False result = new ArrestRate_True_False();
        
        
        public void reduce(NullWritable key, Iterable<ArrestRate_True_False> values, Context context)throws IOException, InterruptedException{
            float sum=0;
            int true_count=0;
            int false_count=0;
            
            for(ArrestRate_True_False val : values){
                true_count += val.getTrue_count();
                false_count += val.getFalse_count();
                sum+= val.getTrue_count() + val.getFalse_count();
            }
            result.setPercentTrue((true_count/sum)*100);
            result.setPercentFalse((false_count/sum)*100);
            result.setTrue_count(true_count);
            result.setFalse_count(false_count);
            
            context.write(key, result);
        }
    
    }
    
}
