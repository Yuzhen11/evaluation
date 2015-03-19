package temporal;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

@Algorithm(name = "shortest", description = "shortest")
public class shortest extends
		Vertex<IntWritable, TemporalWritable, IntWritable, IntWritable> {

	private boolean isSource() {
		return getValue().getOriginalID() == Integer.parseInt(getConf().get("source", "-1"));
    	}
	private void broadcast() {
    		for (Edge<IntWritable, IntWritable> edge : getEdges())
    		{
    			int dist = getValue().getArrivalTime() + edge.getValue().get();
    			sendMessage(edge.getTargetVertexId(), new IntWritable(dist));
    		}
    	}
    	
	@Override
	public void compute(Iterable<IntWritable> messages) {

		if (getSuperstep() == 0 && getValue().getTimestamp() >= 0)
		{	
			TemporalWritable info = new TemporalWritable();
			info = getValue();
			setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(),info.getToOriginalId(), 1000000000,0));
			if (isSource())
			{	
				setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(),0,info.getTimestamp()));
				sendMessage(new IntWritable(getValue().getToOriginalId()), new IntWritable(getValue().getArrivalTime()) );
				broadcast();
			}
		}
		else
		{
			if (getValue().getTimestamp() < 0)
			{
				int mini = 1000000000;
				for (IntWritable message : messages){
					if (message.get() < mini) mini = message.get();
				}
				if (mini < getValue().getArrivalTime()){
					TemporalWritable info = new TemporalWritable();
					info = getValue();
					setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(),info.getToOriginalId(), mini,0) );
				}
			}
			else 
			{
				int mini = 1000000000;
				for (IntWritable message : messages){
					if (message.get() < mini) mini = message.get();
				}
				if (mini < getValue().getArrivalTime())
				{
					TemporalWritable info = new TemporalWritable();
					info = getValue();
					setValue(new TemporalWritable(info.getOriginalID(), info.getTimestamp(), info.getToOriginalId(), mini, 0));
					sendMessage(new IntWritable(getValue().getToOriginalId()), new IntWritable(getValue().getArrivalTime() ) );
					broadcast();
				}
			}
		}
			
		this.voteToHalt();

	}
}
