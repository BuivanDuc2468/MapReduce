import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class KnnMaper extends Mapper<Object, Text, NullWritable, DoubleString>
	{
		DoubleString distanceAndModel = new DoubleString();
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		
		// Declaring some variables which will be used throughout the mapper
		int K;
	    
		double sepal_length;
		double sepal_width;
		double petal_length;
		double petal_width;
	
		private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}
		
		private double totalSquaredDistance(double R1, double R2, double R3, double R4, double S1,double S2, double S3, double S4){	
			double sepal_length_Difference = S1 - R1;
			double sepal_width_Difference = S2 - R2;
			double petal_length_Difference = S3 - R3;
			double petal_width_Difference = S4- R4;
			return squaredDistance(sepal_length_Difference) + squaredDistance(sepal_width_Difference) + squaredDistance(petal_length_Difference) + squaredDistance(petal_width_Difference);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
				String knnParams = FileUtils.readFileToString(new File("C://Users//TrungDuc//eclipse-workspace//KnnMapReducer//input.txt"));
				StringTokenizer st = new StringTokenizer(knnParams, ",");
		    	K = Integer.parseInt(st.nextToken());
				sepal_length = Double.parseDouble(st.nextToken());
				sepal_width = Double.parseDouble(st.nextToken());
				petal_length = Double.parseDouble(st.nextToken());
				petal_width = Double.parseDouble(st.nextToken());
				
		}
				
		@Override
		// The map() method is run by MapReduce once for each row supplied as the input data
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			setup(context);
			String rLine = value.toString();
			StringTokenizer st = new StringTokenizer(rLine, ",");
			
			double normalisedRAge = Double.parseDouble(st.nextToken());
			double normalisedRIncome = Double.parseDouble(st.nextToken());
			double rStatus = Double.parseDouble(st.nextToken());
			double rGender = Double.parseDouble(st.nextToken());
			
			String rModel = st.nextToken();
			
			double tDist = totalSquaredDistance(normalisedRAge, normalisedRIncome, rStatus, rGender, sepal_length, sepal_width, petal_length, petal_width);		
			
			KnnMap.put(tDist, rModel);
			
			
			if (KnnMap.size() > K)
			{
				KnnMap.remove(KnnMap.lastKey());
			}
		}

		@Override
		// The cleanup() method is run once after map() has run for every row
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// Loop through the K key:values in the TreeMap
			for(Map.Entry<Double, String> entry : KnnMap.entrySet())
			{
				  Double knnDist = entry.getKey();
				  String knnModel = entry.getValue();
				  // distanceAndModel is the instance of DoubleString declared aerlier
				  distanceAndModel.set(knnDist, knnModel);
				  // Write to context a NullWritable as key and distanceAndModel as value
				  context.write(NullWritable.get(), distanceAndModel);
			}
		}
	}
