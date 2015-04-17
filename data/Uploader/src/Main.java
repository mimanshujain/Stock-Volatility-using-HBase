import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Main{


	public static void main(String[] args){

		Configuration conf = HBaseConfiguration.create();
		try {
			long start = new Date().getTime();
			HBaseAdmin admin = new HBaseAdmin(conf);
			//Create the Full Data table
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("fullData"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("fullData")){
				admin.disableTable("fullData");
				admin.deleteTable("fullData");
			}
			admin.createTable(tableDescriptor);

			//Create the Intermediate Table
			tableDescriptor = new HTableDescriptor(TableName.valueOf("vol_values"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
			
			if ( admin.isTableAvailable("vol_values")){
				admin.disableTable("vol_values");
				admin.deleteTable("vol_values");
			}
			admin.createTable(tableDescriptor);

			//Create the Final Result table MaxMin
			tableDescriptor = new HTableDescriptor(TableName.valueOf("MaxMin"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
			
			if ( admin.isTableAvailable("MaxMin")){
				admin.disableTable("MaxMin");
				admin.deleteTable("MaxMin");
			}
			admin.createTable(tableDescriptor);
			
			//Job 1 to populate the full data
			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("fullData", null, job);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			
			//Job 2 to start the Actual Mapping Process
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(Main.class);
			Scan scan = new Scan();
			TableMapReduceUtil.initTableMapperJob("fullData", scan, job2.Map.class, Text.class, Text.class, job2);			
			TableMapReduceUtil.initTableReducerJob("vol_values", job2.Reduce.class, job2);
			job2.waitForCompletion( true );
			
			//Job 3 to filter out the top ten results
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(Main.class);
			Scan scan3 = new Scan();
			TableMapReduceUtil.initTableMapperJob("vol_values", scan3, job3.Map.class, DoubleWritable.class, Text.class, job3);
			TableMapReduceUtil.initTableReducerJob("MaxMin", job3.Reduce.class, job3);			
			boolean status = job3.waitForCompletion(true);
			
			//To display the filtered results in the required order
			HTable table = new HTable(conf, "MaxMin");
			Scan topScan = new Scan();
			ResultScanner resultScan = table.getScanner(topScan);
			int count = 0;		
			TreeMap<Double, String> tree = new TreeMap<Double, String>();
			System.out.println("\nThe top 10 stocks with the lowest (min) volatility are\n");
			for(Result value : resultScan)
			{
				tree.put(Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("volatility"), Bytes.toBytes("volatility")))),
						Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name"))));
			}
			Iterator<Double> it = tree.keySet().iterator();
			while(it.hasNext())
			{
				Double key = it.next();
				if(count < 10)
					System.out.println(tree.get(key) + "  " + key);
				
				if(count == 10)
					System.out.println("\nThe top 10 stocks with the highest (max) volatility are\n");
				if(count >= 10)
					System.out.println(tree.get(key) + "  " + key);
				count++;
			}
			
			if (status == true) {
				long end = new Date().getTime();
				System.out.println("\nThe time taken by job is " + (end-start)/1000 + " Seconds\n");
			}
			table.close();
			admin.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}


