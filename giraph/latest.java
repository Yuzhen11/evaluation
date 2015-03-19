package temporal;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

@Algorithm(name = "latest", description = "latest")
public class latest extends
		Vertex<IntWritable, TemporalWritable, IntWritable, IntWritable> {

	private boolean isSource() {
		return getValue().getOriginalID() == Integer.parseInt(getConf().get("source", "-1"));
    	}
	private void broadcast() {
    		sendMessageToAllEdges(new IntWritable(0));
    	}
    	
	@Override
	public void compute(Iterable<IntWritable> messages) {
		if (getSuperstep() == 0 && getValue().getTimestamp() >= 0)
		{	
			TemporalWritable info = new TemporalWritable();
			info = getValue();
			setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(), -1, 0));
			if (isSource())
			{
				//TemporalWritable info = new TemporalWritable();
				//info = getValue();
				setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(), 0, 1));
				sendMessage(new IntWritable(getValue().getToOriginalId()), new IntWritable(getValue().getTimestamp()) );
				broadcast();
			}
		}
		else
		{
			if (getValue().getTimestamp() < 0)
			{
				TemporalWritable info = new TemporalWritable();
				info = getValue();
				if (isSource() )
				{
					setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(),0,1));
				}
				else
				{
					int maxi = -1;
					for (IntWritable message : messages){
						if (message.get() > maxi) maxi = message.get();
					}
					if (maxi > getValue().getArrivalTime()) {
						setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(), maxi,1));
					}
				}
			}
			else if (getValue().getVis() == 0)
			{
				TemporalWritable info = new TemporalWritable();
				info = getValue();
				setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId() ,0,1));
				sendMessage(new IntWritable(getValue().getToOriginalId()), new IntWritable(getValue().getTimestamp()));
				broadcast();
			}
		}
			
		this.voteToHalt();

	}
}
