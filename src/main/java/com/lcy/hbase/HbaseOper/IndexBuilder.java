package com.lcy.hbase.HbaseOper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexBuilder {
	//索引表唯一的一列为INDEX:ROW, 其中INDEX为列族
	public static final byte[] INDEX_COLUMN = Bytes.toBytes("INDEX");
	
	public static final byte[] INDEX_QUALIFIER = Bytes.toBytes("ROW");
	
	
	public static class Map extends 
	Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable,Writable>{
		private byte[] family;
		private HashMap<byte[], ImmutableBytesWritable> indexes;
		
		
		//mapper中的方法， 只在任务初始化时执行一次
				@Override
				protected void setup(
						Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Writable>.Context context)
						throws IOException, InterruptedException {
					Configuration configuration = context.getConfiguration();
					String tableName = configuration.get("index.tablename");
					String[] fields = configuration.getStrings("index.fields");
					String familyName = configuration.get("index.familyname");
					family = Bytes.toBytes(familyName);
					indexes = new HashMap<byte[],ImmutableBytesWritable>();
					for(String field: fields){
						indexes.put(Bytes.toBytes(field), 
								new ImmutableBytesWritable(Bytes.toBytes(tableName+"-"+field)));
								
					}
				}
		
		
		
		/**
		 * rowKey: 行健， result： 为该行所包含的数据
		 */
		@Override
		protected void map(
				ImmutableBytesWritable rowKey,
				Result result,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Writable>.Context context)
				throws IOException, InterruptedException {
			
			for(java.util.Map.Entry<byte[], ImmutableBytesWritable> index : indexes.entrySet()){
				//获得列名
				byte[] qualifier = index.getKey();
				//索引表的表名
				ImmutableBytesWritable tableName = index.getValue();
				//根据“列族：列名”获取元素值
				byte[] value = result.getValue(family, qualifier);
				
				if(value !=null){
					//以列值作为行健，在列INDEX:ROW 中插入行键
					Put put = new Put(value);
					//在tablename 表上执行put 操作。
					put.add(INDEX_COLUMN , INDEX_QUALIFIER, rowKey.get());
					/*context.write(tableName, put);*/
					
				}
			}
			
		}

	}
	
	
	
	
	public static void main(String[] args) {
		
	}

}
