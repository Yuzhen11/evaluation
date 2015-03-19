package temporal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class TemporalWritable implements Writable {
	private int originalID;
	private int timestamp;
	private int toOriginalId;
	private int arrivalTime;
	private int vis;
	
	public TemporalWritable(int originalID, int timestamp, int toOriginalId, int arrivalTime, int vis) {

		this.originalID = originalID;
		this.timestamp = timestamp;
		this.toOriginalId = toOriginalId;
		this.arrivalTime = arrivalTime;
		this.vis = vis;
	}
	public TemporalWritable(){
	
	}
	public int getOriginalID()
	{
		return originalID;
	}
	public int getTimestamp()
	{
		return timestamp;
	}
	public int getToOriginalId()
	{
		return toOriginalId;
	}
	public int getArrivalTime()
	{
		return arrivalTime;
	}
	public int getVis()
	{
		return vis;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(originalID);
		out.writeInt(timestamp);
		out.writeInt(toOriginalId);
		out.writeInt(arrivalTime);
		out.writeInt(vis);
	}

	public void readFields(DataInput in) throws IOException {
		originalID = in.readInt();
		timestamp = in.readInt();
		toOriginalId = in.readInt();
		arrivalTime = in.readInt();
		vis = in.readInt();
	}

	public static TemporalWritable read(DataInput in) throws IOException {
		TemporalWritable w = new TemporalWritable();
		w.readFields(in);
		return w;
	}
}
