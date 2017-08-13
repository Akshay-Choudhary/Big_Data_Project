/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Count;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author akshay
 */
public class Count_Main {
    
    
    public static void main( String[] args )
    {
        try
        {
            // Setup Hadoop
            Configuration conf = HBaseConfiguration.create();
            Job job = Job.getInstance(conf, "ChicagoCrimes");
            job.setJarByClass(Count_Main.class );

            // Create a scan
            Scan scan = new Scan();

            // Configure the Map process to use HBase
            TableMapReduceUtil.initTableMapperJob(

                    "ChicagoCrimes",                    // The name of the table
                    scan,                           // The scan to execute against the table
                    testMapper.class,                 // The Mapper class
                    LongWritable.class,             // The Mapper output key class
                    LongWritable.class,             // The Mapper output value class
                    job );                          // The Hadoop job

            // Configure the reducer process
            job.setReducerClass( MyReducer.class );
            job.setCombinerClass( MyReducer.class );

            // Setup the output - we'll write to the file system: HOUR_OF_DAY   PAGE_VIEW_COUNT
            job.setOutputKeyClass( LongWritable.class );
            job.setOutputValueClass( LongWritable.class );
            job.setOutputFormatClass( TextOutputFormat.class );

            // We'll run just one reduce task, but we could run multiple
            job.setNumReduceTasks( 1 );

            // Write the results to a file in the output directory
            FileOutputFormat.setOutputPath( job, new Path(args[0]));

            // Execute the job
            System.exit( job.waitForCompletion( true ) ? 0 : 1 );

        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
    }
    
    
    
    public static class testMapper extends TableMapper<LongWritable,LongWritable> {
        
        private LongWritable ONE = new LongWritable( 1 );
 
        public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
          throws IOException, InterruptedException {

         try {
          // get rowKey and convert it to string
          String inKey = new String(rowKey.get());
          // set new key having only date
          // get sales column in byte format first and then convert it to string (as it is stored as string from hbase shell)
          byte[] fbi_n = columns.getValue(Bytes.toBytes("FBI_Code"), Bytes.toBytes("FBI_Code"));
          String FBI_Code = new String(fbi_n);
          Long f= new Long(FBI_Code);
          // emit date and sales values
          context.write(new LongWritable(f), ONE);
         } catch (RuntimeException e){
          e.printStackTrace();
         }
        }
    }
    
    
    public static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>

    {
        public MyReducer() {
        }

        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException

        {
            // Add up all of the page views for this hour
            long sum = 0;
            for( LongWritable val : values)
            {
                sum+= val.get();
            }

            // Write out the current hour and the sum
            context.write( key, new LongWritable(sum));
        }
    }
    
    
    
    
}
