import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class job2 {
	

	public static class Map extends TableMapper<Text, Text>{
		
		private Text name = new Text();
		private Text datePrice = new Text();

		private static String prevCom = null;
		private static String prevMonth = null;
		private static String prevDay =  null;
		private static String  prevYear = null;
		private static String prevAdjPrice = null;
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result value, Context context) throws IOException, InterruptedException {
			
			name.set("");
			datePrice.set("");

			String companyKey = Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
			String year = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr")));
			String month = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm")));
			String day = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd")));
			String adjClosePr = Bytes.toString(value.getValue(Bytes.toBytes("price"), Bytes.toBytes("price")));
			
			if(prevCom == null)
			{
				prevCom = companyKey;
				prevAdjPrice = adjClosePr;
				prevDay = day;
				prevMonth = month;
				prevYear = year;
				
				name.set(companyKey);
				datePrice.set(day + " " + adjClosePr + " " + year + " " + month);				
				context.write(name, datePrice);	
			}
			
			else
			{
				if(prevCom.equals(companyKey) && prevMonth.equals(month)
						&& prevYear.equals(year))
				{
					prevDay = day;
					prevAdjPrice = adjClosePr;
					return;
				}
				
				else
				{
					name.set(prevCom);

					datePrice.set(prevDay + " " + prevAdjPrice + " "
							+ prevYear + " " + prevMonth);
					
					context.write(name, datePrice);
					name.set("");
					datePrice.set("");
					
					prevCom = companyKey;
					prevAdjPrice = adjClosePr;
					prevDay = day;
					prevMonth = month;
					prevYear = year;

					name.set(companyKey);
					datePrice.set(day + " " + adjClosePr + " " + year + " " + month);	
					context.write(name, datePrice);	
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			if(prevCom != null && !prevCom.equals(""))
			{	
				name.set(prevCom);
				datePrice.set(prevDay + " " + prevAdjPrice + " "
						+ prevYear + " " + prevMonth);
				context.write(name, datePrice);
			}

		}
	}
	

	public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable>{
		int firstDay = 0, lastDay = 0;
		String firstPrice = "";
		String lastPrice = "";
		int count = 1;
		
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
			HashMap<String, TreeMap<String, String>> map = new HashMap<String, TreeMap<String,String>>();

			Iterator<Text> it = values.iterator();
			String[] data;
			String name = "";
			String day = "";
			String adjClose = "";
			ArrayList<Double> diffList = new ArrayList<Double>();
			TreeMap<String, String> tree = null;
			int c = 0;
			while(it.hasNext())
			{

				data = it.next().toString().split(" ");

				name = key.toString()+" "+data[2]+" "+data[3];
				day = data[0];

				adjClose = data[1];
				if(map.containsKey(name))
				{
					tree = map.get(name);
					tree.put(day, adjClose);
				}
				else
				{
					tree = new TreeMap<String, String>();
					tree.put(day, adjClose);
					map.put(name, tree);
				}
				tree = null;
				c++;
			}

			tree = null;
			Iterator<String> it2 = map.keySet().iterator();
			double first= 0D;
			double last = 0D;
			double diff = 0D;
			while(it2.hasNext())
			{
				String k = it2.next();
				tree = map.get(k);

				firstPrice = tree.get(tree.firstKey());
				lastPrice = tree.get(tree.lastKey());

				first = Double.parseDouble(firstPrice);
				last = Double.parseDouble(lastPrice);
				diff = (last - first)/first;
				diffList.add(diff);

				tree = null;
			}
			double sum = 0D;
			int counter = 0;
			Iterator<Double> it3 = diffList.iterator();

			while(it3.hasNext())
			{

				sum = sum + it3.next();
				counter++;
			}

			sum = sum/counter;

			it3 = diffList.iterator();
			double addDiff = 0D;
			while(it3.hasNext())
			{
				diff = 0D;
				diff = it3.next() - sum;
				diff = Math.pow(diff, 2.0);
				addDiff = addDiff + diff;
			}

			if(counter > 1)
				addDiff = addDiff/(counter - 1);
			if(addDiff != 0.0)
			{

				byte[] rowid = Bytes.toBytes(key.toString() + count++ +"");
				Text dummy = new Text(Math.sqrt(addDiff)+"");
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
				p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"),Bytes.toBytes(dummy.toString()));
				context.write(null, p);//new ImmutableBytesWritable(rowid)
				firstPrice = "";
				lastPrice = "";
			}
		}
		
	}
}