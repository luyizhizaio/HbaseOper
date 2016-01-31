package com.lcy.hbase.crud;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCURDTest {
	         
	
	//hbase操作必备
    private static Configuration getConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://yarn1:9000/hbase");
		//使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "yarn1,yarn2,yarn3");
		return conf;
	}
  //创建一张表
    public static void create(String tableName, String ... columnFamilys) throws IOException{
    	HBaseAdmin admin = new HBaseAdmin(getConfiguration());
    	if (admin.tableExists(tableName)) {
    		System.out.println("table exists!");
    	}else{
    		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    		for(String columnFamily:columnFamilys){
    			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
    		}
    		admin.createTable(tableDesc);
    		System.out.println("create table success!");
    	}
    }
	
	
	//添加一条记录
	public static void put(String tableName, String row, String columnFamily, String column, String data) throws IOException{
		HTable table = new HTable(getConfiguration(), tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), 	Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'"+row+"',"+columnFamily+":"+column+"','"+data+"'");
	}

	//读取一条记录
	public static Result get(String tableName, String row) throws IOException{
		HTable table = new HTable(getConfiguration(), tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		System.out.println("Get: "+result);
		return result;
	}

	//显示所有数据
	public static ResultScanner scan(String tableName) throws IOException{
		HTable table = new HTable(getConfiguration(), tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		/*for (Result result : scanner) {
			System.out.println("Scan: "+result);
		}*/
		return  scanner;
	}

	//删除表
	public static void delete(String tableName) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if(admin.tableExists(tableName)){
			try {
			  admin.disableTable(tableName);
			  admin.deleteTable(tableName);
			} catch (IOException e) {
			  e.printStackTrace();
			  System.out.println("Delete "+tableName+" 失败");
			}
		}
		System.out.println("Delete "+tableName+" 成功");
	}
	
	
	



	
	public static void main(String[] args) throws IOException {
		String tableName="access_ip_num";
		String columnFamily="detail";
		
		HbaseCURDTest.create(tableName, columnFamily);
		/*HbaseCURDTest.put(tableName, "row1", columnFamily, "cl1", "data");
		HbaseCURDTest.get(tableName, "row1");*/
		/*ResultScanner scanner = HbaseCURDTest.scan(tableName);
		for (Result row : scanner) {
    		System.out.format("Row\t%s\n", new String(row.getRow()));
			for(Map.Entry<byte[], byte[]> entry : row.getFamilyMap("cf".getBytes()).entrySet()){
				
				String column = new String(entry.getKey());
				String value = new String(entry.getValue());
				System.out.format("COLUMN\tdetail:%s\t%s\n",column,value);
			}
		}*/
		/*HbaseCURDTest.delete(tableName);*/
	}
	
	/*public static void main(String[] args) throws IOException {
		String tableName="order_detail";
		String columnFamily="detail";
		String phone = "1888888888";
		String apmac = "de:12:ee:d1:e3:33";
		String host = "192.1.1.34";
		String httpStatus = "200";
		String upPackNum = "250";
		
		Date date = new Date();
		String rowkey = DateFormatUtils.format(date, "yyyyMMddHHmmss");
		
		HbaseCURDTest.create(tableName, columnFamily);
		HbaseCURDTest.put(tableName, rowkey, columnFamily, "reportTime", rowkey);
		HbaseCURDTest.put(tableName, rowkey, columnFamily, "phone", phone);
		HbaseCURDTest.put(tableName, rowkey, columnFamily, "apmac", apmac);
		HbaseCURDTest.put(tableName, rowkey, columnFamily, "host", host);
		HbaseCURDTest.put(tableName, rowkey, columnFamily, "httpStatus", httpStatus);
		HbaseCURDTest.put(tableName, rowkey, columnFamily, "upPackNum", upPackNum);
		//GET
		Result result = HbaseCURDTest.get(tableName, rowkey);
		Cell cell = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("host"));
		String gethost = new String(cell.getValue());
		System.out.println("host:"+gethost);
		//SCANNER
		ResultScanner scanner = HbaseCURDTest.scan(tableName);
		for (Result row : scanner) {
    		System.out.format("Row\t%s\n", new String(row.getRow()));
			for(Map.Entry<byte[], byte[]> entry : row.getFamilyMap("detail".getBytes()).entrySet()){
				
				String column = new String(entry.getKey());
				String value = new String(entry.getValue());
				System.out.format("COLUMN\tdetail:%s\t%s\n",column,value);
			}
		}
		HbaseCURDTest.delete(tableName);
	}*/
}
