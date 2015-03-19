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

@Algorithm(name = "fastest", description = "fastest")
public class fastest extends
		Vertex<IntWritable, TemporalWritable, IntWritable, IntWritable> {

	private boolean isSource() {
		return getValue().getOriginalID() == Integer.parseInt(getConf().get("source", "-1"));
    	}
	private void broadcast(int startTime) {
    		sendMessageToAllEdges(new IntWritable(startTime));
    	}
    	
	@Override
	public void compute(Iterable<IntWritable> messages) {

		if (getSuperstep() == 0 && getValue().getTimestamp() >= 0)
		{	
			TemporalWritable info = new TemporalWritable();
			info = getValue();
			setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(),info.getToOriginalId(), 1000000000,-1));
			if (isSource())
			{	
				setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(),0,info.getTimestamp()));
				sendMessage(new IntWritable(getValue().getToOriginalId()), new IntWritable(-(getValue().getTimestamp()-getValue().getVis())) );
				broadcast(getValue().getVis());
			}
		}
		else
		{
			if (getValue().getTimestamp() < 0)
			{
				int maxi = -1000000000;
				for (IntWritable message : messages){
					if (message.get() > maxi) maxi = message.get();
				}
				maxi = -maxi;
				if (maxi < getValue().getArrivalTime()){
					TemporalWritable info = new TemporalWritable();
					info = getValue();
					setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(),info.getToOriginalId(), maxi,1) );
				}
			}
			else 
			{
				int maxi = -1;
				for (IntWritable message : messages){
					if (message.get() > maxi) maxi = message.get();
				}
				if (maxi > getValue().getVis())
				{
					TemporalWritable info = new TemporalWritable();
					info = getValue();
					setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(), info.getArrivalTime(), maxi));
					sendMessage(new IntWritable(getValue().getToOriginalId()), new IntWritable(-(getValue().getTimestamp()-getValue().getVis() )) );
					broadcast(getValue().getVis());
				}
			}
		}
			
		this.voteToHalt();

	}
}
