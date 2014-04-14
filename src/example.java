import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class Samples implements Writable{
	
	private static final Log log = LogFactory.getLog(Samples.class);
	private static final int DIM = 60;
	private double arr[];
	
	public Samples(){
		arr = new double[DIM];
	}
	
	public static double getEulerDist(Samples vec1, Samples vec2){
		double dist = 0.0;
		for(int i=0;i<DIM;i++){
			dist+=(vec1.arr[i]-vec2.arr[i])*(vec1.arr[i]-vec2.arr[i]);
		}
		return Math.sqrt(dist);
	}
	public static int getDIM(){
		return DIM;
	}
	public double [] getarr()
	{
		return arr;
	}
	public void clear(){
		for(int i=0;i<arr.length;i++)
			arr[i]=0.0;
	}
	@Override
	public String toString(){
		String rect = String.valueOf(arr[0]);
		for(int i=0;i<DIM;i++)
			rect+="\t"+String.valueOf(arr[i]);
		return rect;
	}
	@Override
	public void readFields(DataInput in) throws IOException{
		String str[]=in.readUTF().split("\\s+");
		for(int i=0;i<DIM;i++)
			arr[i]=Double.parseDouble(str[i]);
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeUTF(this.toString());
	}
}
