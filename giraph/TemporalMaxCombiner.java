package temporal;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.IntWritable;

public class TemporalMaxCombiner extends Combiner<IntWritable, IntWritable> {
    @Override
    public void combine(IntWritable vertexIndex, IntWritable originalMessage,
	    IntWritable messageToCombine) {
	originalMessage.set(Math.max(originalMessage.get(),
		messageToCombine.get()));
    }

    @Override
    public IntWritable createInitialMessage() {
	return new IntWritable(Integer.MAX_VALUE);
    }
}
