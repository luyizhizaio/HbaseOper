package com.lcy.hbase.HbaseOper;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception, ZooKeeperConnectionException
    {
    	//初始化hbase配置
    	Configuration conf = HBaseConfiguration.create();
    	//指定zk集群
    	conf.set("hbase.zookeeper.quorum", "yarn1,yarn2,yarn3");
    	//操作元数据的类，对表进行操作
    	HBaseAdmin admin = new HBaseAdmin(conf);
    
    	//1.创建新表
    	HTableDescriptor tableDescriptor = new HTableDescriptor("user2".getBytes());
    	//添加列族
    	tableDescriptor.addFamily(new HColumnDescriptor("info"));
    	tableDescriptor.addFamily(new HColumnDescriptor("address"));
    	admin.createTable(tableDescriptor);
    	//表对象
    	HTable table = new HTable(conf,"user2");
    	
    	//2.Htable.put()插入数据,rowone1为行键
    	Put putRow1 = new Put("jiangxiaoyue".getBytes());
    	putRow1.add("info".getBytes(),"age".getBytes(),"25".getBytes());
    	putRow1.add("info".getBytes(),"sex".getBytes(),"male".getBytes());
    	putRow1.add("address".getBytes(),"prevince".getBytes(),"anhui".getBytes());
    	putRow1.add("address".getBytes(),"city".getBytes(),"hefei".getBytes());
    	table.put(putRow1);
    	
    	System.out.println("add row2");
    	Put putRow2 = new Put("xiaoli".getBytes());//row2为行键
    	putRow2.add("info".getBytes(),"age".getBytes(), "21".getBytes());
    	putRow2.add("info".getBytes(),"sex".getBytes(), "female".getBytes());
    	table.put(putRow2);
    	
    	//4.htable.getScanner()获得数据，该方法返回Result类.
    	ResultScanner resultScanner = table.getScanner("info".getBytes());
    	for (Result row : resultScanner) {
    		System.out.format("Row\t%s\n", new String(row.getRow()));
			for(Map.Entry<byte[], byte[]> entry : row.getFamilyMap("info".getBytes()).entrySet()){
				
				String column = new String(entry.getKey());
				String value = new String(entry.getValue());
				System.out.format("COLUMN\tinfo:%s\t%s\n",column,value);
			}
		}
    	
    	//5.删除表
    	/*System.out.println("删除表 lcytab");
    	admin.disableTable("lcytab");
    	admin.deleteTable("lcytab");*/
    }
}
