/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 *
 * @author akshay
 */
    public class HBaseBulkLoad {	
    /**
     * doBulkLoad.
     *
     * @param pathToHFile path to hfile
     * @param tableName 
     */
    public static void doBulkLoad(String pathToHFile, String tableName) {
        try {		
            Configuration configuration = new Configuration();			
            configuration.set("mapreduce.child.java.opts", "-Xmx300m");	
            HBaseConfiguration.addHbaseResources(configuration);	
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);	
            HTable hTable = new HTable(configuration, tableName);	
            loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);	
            System.out.println("Bulk Load Completed..");		
        } catch(Exception exception) {			
            exception.printStackTrace();			
        }		
    }	
}
