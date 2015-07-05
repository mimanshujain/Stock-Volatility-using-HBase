
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
				
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("fullData"));
				tableDescriptor.addFamily(new HColumnDescriptor("stock"));
				tableDescriptor.addFamily(new HColumnDescriptor("time"));
				tableDescriptor.addFamily(new HColumnDescriptor("price"));
				if ( admin.isTableAvailable("fullData")){
					admin.disableTable("fullData");
					admin.deleteTable("fullData");
				}
				admin.createTable(tableDescriptor);

				Job job = Job.getInstance();
				job.setJarByClass(Main.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				//			FileInputFormat.addInputPath(job, new Path("/home/sherlock/Dropbox/SecondSem/DIC/ProjectData/s"));
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(Job1.Map.class);
				TableMapReduceUtil.initTableReducerJob("fullData", null, job);
				job.setNumReduceTasks(0);
				job.waitForCompletion(true);

				admin.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}


