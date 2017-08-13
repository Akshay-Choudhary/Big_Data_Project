/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author akshay
 */
public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String hbaseTable;	
    private String dataSeperator;
    private String columnFamily1;
    private String columnFamily2;
    private ImmutableBytesWritable hbaseTableName;
    

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();		
        hbaseTable = configuration.get("hbase.table.name");		
        dataSeperator = configuration.get("data.seperator");		
        columnFamily1 = configuration.get("COLUMN_FAMILY_1");		
        columnFamily2 = configuration.get("COLUMN_FAMILY_2");		
        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
//        System.out.println("In Mapper");
//        System.out.println(columnFamily1);
//        System.out.println(columnFamily2);
//        System.out.println(hbaseTable);
        
    }

    public void map(LongWritable key, Text value, Context context) {
        try {		
            String[] values = value.toString().split(dataSeperator);
            //System.out.println(values[0]);
            
            String rowKey = values[0];			
            Put put = new Put(Bytes.toBytes(rowKey));	
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Case Number"), Bytes.toBytes(values[1]));
//            System.out.println(columnFamily1);
//            System.out.println(values[1]);
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Date"), Bytes.toBytes(values[2]));			
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Block"), Bytes.toBytes(values[3]));
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("IUCR"), Bytes.toBytes(values[4]));			
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Primary Type"), Bytes.toBytes(values[5]));
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Description"), Bytes.toBytes(values[6]));			
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Location Description"), Bytes.toBytes(values[7]));
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Arrest"), Bytes.toBytes(values[8]));			
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Domestic"), Bytes.toBytes(values[9]));
//            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Beat"), Bytes.toBytes(values[10]));
            
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("District"), Bytes.toBytes(values[11]));			
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Ward"), Bytes.toBytes(values[12]));
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Community Area"), Bytes.toBytes(values[13]));			
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("FBI Code"), Bytes.toBytes(values[14]));
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("X Coordinate"), Bytes.toBytes(values[15]));			
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Y Coordinate"), Bytes.toBytes(values[16]));
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Year"), Bytes.toBytes(values[17]));			
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Updated On"), Bytes.toBytes(values[18]));
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("District"), Bytes.toBytes(values[19]));			
//            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Latitude"), Bytes.toBytes(values[20]));
            //put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("Longitude"), Bytes.toBytes(values[21]));
       
            
            context.write(hbaseTableName, put);			
        } catch(Exception exception) {			
            exception.printStackTrace();
            System.out.println("Exception is here" + exception);
            
        }
    }
}
