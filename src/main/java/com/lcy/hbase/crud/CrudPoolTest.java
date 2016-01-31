package com.lcy.hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class CrudPoolTest {
	
	
	public static HTablePool getHtablePool(){
		Configuration conf = HBaseConfiguration.create();
		HTablePool hTablePool = new HTablePool(conf, 10);
		return hTablePool;
	}
	
	
	
	public static void createTable(String tableName){
		HTableInterface table = getHtablePool().getTable(tableName);
		//table.
	}

}
