
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.*;

public class Main{


	public static void main(String[] args){

		Configuration conf = HBaseConfiguration.create();
		if(conf != null)
		{
			try {
				HBaseAdmin.checkHBaseAvailable(conf);
				HBaseAdmin admin = new HBaseAdmin(conf);
				;
//				while(admin == null)
//				{
//					admin = new HBaseAdmin(conf);
//				}
//				admin = org.apache.hadoop.hbase.client.
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("fullData"));
				tableDescriptor.addFamily(new HColumnDescriptor("stock"));
				tableDescriptor.addFamily(new HColumnDescriptor("time"));
				tableDescriptor.addFamily(new HColumnDescriptor("price"));
				if ( admin.isTableAvailable("fullData")){
					admin.disableTable("fullData");
					admin.deleteTable("fullData");
				}
				admin.createTable(tableDescriptor);

//				tableDescriptor = new HTableDescriptor(TableName.valueOf("vol_values"));
//				tableDescriptor.addFamily(new HColumnDescriptor("stock"));
//				//			tableDescriptor.addFamily(new HColumnDescriptor("date"));
//				tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
//				if ( admin.isTableAvailable("vol_values")){
//					admin.disableTable("vol_values");
//					admin.deleteTable("vol_values");
//				}
//				admin.createTable(tableDescriptor);
//
//				tableDescriptor = new HTableDescriptor(TableName.valueOf("MaxMin"));
//				tableDescriptor.addFamily(new HColumnDescriptor("stock"));
//				//			tableDescriptor.addFamily(new HColumnDescriptor("date"));
//				tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
//				if ( admin.isTableAvailable("MaxMin")){
//					admin.disableTable("MaxMin");
//					admin.deleteTable("MaxMin");
//				}
//				admin.createTable(tableDescriptor);

				Job job = Job.getInstance();
				job.setJarByClass(Main.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				//			FileInputFormat.addInputPath(job, new Path("/home/sherlock/Dropbox/SecondSem/DIC/ProjectData/s"));
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(Job1.Map.class);
				TableMapReduceUtil.initTableReducerJob("fullData", null, job);
				job.setNumReduceTasks(0);
				job.waitForCompletion(true);
//				//			table.close();
//
//				Job job2 = Job.getInstance(conf);
//				job2.setJarByClass(Main.class);
//				Scan scan = new Scan();
//				TableMapReduceUtil.initTableMapperJob("fullData", scan, job2.Map.class, Text.class, Text.class, job2);
//				//			job2.setReducerClass( job2.Reduce.class );
//				//            job2.setCombinerClass( job2.Reduce.class );
//				job2.setNumReduceTasks(1);
//				TableMapReduceUtil.initTableReducerJob("vol_values", job2.Reduce.class, job2);
//
//				// Execute the job
//				job2.waitForCompletion( true );
//				//"fullData", scan, job2.Map.class, Text.class, Text.class, job2
//
//				Job job3 = Job.getInstance(conf);
//				job2.setJarByClass(Main.class);
//				Scan scan3 = new Scan();
//				TableMapReduceUtil.initTableMapperJob("vol_values", scan3, job3.Map.class, DoubleWritable.class, Text.class, job3);
//				job3.setNumReduceTasks(1);
//				TableMapReduceUtil.initTableReducerJob("vol_values", job2.Reduce.class, job3);
//				System.exit(job3.waitForCompletion(true) ? 0 : 1);
				admin.close();

				//			System.out.println("mm");
			}
			catch (Exception e) {

				e.printStackTrace();
				//			e.getCause();
			}
		}
	}
}


//HTable table = new HTable(conf, "fullData");
//
////File ipDirectory = new File(args[0]);
////String[] files = ipDirectory.list();
//BufferedReader buffer = null;
//Path pp = new Path(args[0]);
//org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new Configuration());
//RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(pp, true);
//
//FileStatus[] status = fs.listStatus(pp);
//
//for(int i =0; i< status.length; i++)
//{
////while(fileStatusListIterator.hasNext()){
////    LocatedFileStatus fileStatus = fileStatusListIterator.next();
//	Path p = status[i].getPath();
////    Path p = fileStatus.getPath();
//	buffer = new BufferedReader(new InputStreamReader(fs.open(p)));
//	String line = buffer.readLine();
//	if(line.trim().contains("Date"))
//		line = buffer.readLine();
//	int key = 1;
//	while(line != null)
//	{
//		String[] element = line.split(",");
//		String dates[]= element[0].split("-");
//		byte[] yr = Bytes.toBytes(dates[0]);
//		byte[] mm = Bytes.toBytes(dates[1]);
//		byte[] dd = Bytes.toBytes(dates[2]);
//		byte[] price = Bytes.toBytes(element[6]);
//		String stockName = p.getName();
//		byte[] file = Bytes.toBytes(stockName);
//		byte[] rowid = Bytes.toBytes(stockName + key+"");
//		key++;
//		Put putOp = new Put(rowid);
//		putOp.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), file);
//		putOp.add(Bytes.toBytes("date"), Bytes.toBytes("yr"), yr);
//		putOp.add(Bytes.toBytes("date"), Bytes.toBytes("mm"), mm);
//		putOp.add(Bytes.toBytes("date"), Bytes.toBytes("dd"), dd);
//		putOp.add(Bytes.toBytes("price"), Bytes.toBytes("price"), price);
//		table.put(putOp);
//	}
//}
//

