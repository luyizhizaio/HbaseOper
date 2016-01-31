package com.lcy.hbase.crud;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class BatchImport2Hbase {
	
	public static class BatchImportMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			//61.50.141.7     20130530175451  uc_server/data/avatar/000/00/16/12_avatar_small.jpg HTTP/1.1
			
			String[] split = value.toString().split("\t");
			String rowkey = split[0]+":"+split[1];
			v2.set(rowkey + "\t" + value.toString());
			context.write(key, v2);
		}
		/***
		          行键   ip:date
			明细列族   cf:all
		 */
	}
	
	public static class BatchImportReducer extends TableReducer<LongWritable, Text, NullWritable>{
		//61.50.141.7     20130530175451  uc_server/data/avatar/000/00/16/12_avatar_small.jpg HTTP/1.1
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			for(Text text:values){
				String[] split = text.toString().split("\t");
				final Put put = new Put(Bytes.toBytes(split[0]));
				put.add(Bytes.toBytes("cf"),Bytes.toBytes("ip"),Bytes.toBytes(split[1]));
				put.add(Bytes.toBytes("cf"),Bytes.toBytes("date"),Bytes.toBytes(split[2]));
				put.add(Bytes.toBytes("cf"),Bytes.toBytes("url"),Bytes.toBytes(split[3]));
				context.write(NullWritable.get(),put);
			}
		}
		
	}

	public static void main(String[] args) throws Exception {
		final Configuration configuration = new Configuration();
		//设置zookeeper
		configuration.set("hbase.zookeeper.quorum", "yarn1,yarn2,yarn3");
		//设置hbase表名称
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "access_detail");
		//将该值改大，防止hbase超时退出
		configuration.set("dfs.socket.timeout", "180000");
		Job job = Job.getInstance(configuration);
		
		job.setMapperClass(BatchImportMapper.class);
		job.setReducerClass(BatchImportReducer.class);
		//设置map的输出，不设置reduce的输出类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		//不再设置输出路径，而是设置输出格式类型
		job.setOutputFormatClass(TableOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, "hdfs://yarn1:9000/hmbbs_cleaned/2013_05_30");
		
		job.waitForCompletion(true);
	}
}
