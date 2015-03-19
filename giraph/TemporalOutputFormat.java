package temporal;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

public class TemporalOutputFormat extends
		TextVertexOutputFormat<IntWritable, TemporalWritable, IntWritable> {
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new TemporalVertexWriter();
	}

	/**
	 * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	 */
	protected class TemporalVertexWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<IntWritable, TemporalWritable, IntWritable, ?> vertex)
				throws IOException {
			if (vertex.getValue().getTimestamp() < 0)
			{
				StringBuilder str = new StringBuilder();
			
				str.append(vertex.getValue().getOriginalID());
				str.append(delimiter);
				str.append(vertex.getValue().getArrivalTime());
				return new Text(str.toString());	
			}
			else return new Text("");
		}
	}
}
