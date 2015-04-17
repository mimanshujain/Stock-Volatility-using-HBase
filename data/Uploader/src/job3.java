import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class job3 {

	public static class Map extends TableMapper<DoubleWritable, Text>{
		public void map(ImmutableBytesWritable rowkey, Result value, Context context) throws IOException, InterruptedException {
			
			double val = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"))));
			context.write(new DoubleWritable(val), new Text(Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")))));
		}
	}
	
	public static class Reduce extends TableReducer<DoubleWritable, Text, ImmutableBytesWritable>{
		
		static int count = 0;
		static int counter = 0;
		static long time;
		static HashMap<String, Double> map = new HashMap<String, Double>();
		static String keyVal = "";
		
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
			for(Text value : values)
			{
				
				if(counter < 10)
				{
					byte[] rowid = Bytes.toBytes(value.toString() + counter +"");
					Put p = new Put(rowid);
					p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(value.toString()));
					//p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"),Bytes.toBytes(key.toString()));
					p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"),Bytes.toBytes(Double.toString(key.get())));
					context.write(null, p);//new ImmutableBytesWritable(rowid)
				}

				else
					map.put(value.toString(), key.get());
					
				counter++;
			}	
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			ValueComparator val = new ValueComparator(map);
			TreeMap<String, Double> tree = new TreeMap<String, Double>(val);
			tree.putAll(map);
			int c = 0;
			Iterator<String> it = tree.keySet().iterator();

			String str = "";
			while(it.hasNext())
			{
				String key = it.next();
				str = str + "\n" + key + "     " +  map.get(key);
				byte[] rowid = Bytes.toBytes(key + c +"");
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));

				p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"),Bytes.toBytes(map.get(key).toString()));
				context.write(null, p);//new ImmutableBytesWritable(rowid)
				c++;
				if(c > 9)
					break;
			}
		}
		
		class ValueComparator implements Comparator<String>
		{
			HashMap<String, Double> map;
			
			public ValueComparator(HashMap<String, Double> map) {
				this.map = map;
			}
			
			@Override
			public int compare(String o1, String o2) {
				
				if(map.get(o1) >= map.get(o2))
				{
					return -1;
				}
				
				else return 1;
			}
			
		}
	}
}