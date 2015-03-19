package temporal;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Input format for HashMin IntWritable, NullWritable, NullWritable Vertex ,
 * Vertex Value, Edge Value Graph vertex \t neighbor1 neighbor 2
 */
public class TemporalInputFormat extends
		TextVertexInputFormat<IntWritable, TemporalWritable, IntWritable> {
	/** Separator of the vertex and neighbors */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	public static int SOURCE_VERTEX;
	public static String SOURCE_STRING;

	@Override
	public TextVertexReader createVertexReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		return new TemporalVertexReader();
	}

	/**
	 * Vertex reader associated with {@link IntIntNullTextInputFormat}.
	 */
	public class TemporalVertexReader extends
			TextVertexReaderFromEachLineProcessed<String[]> {

		private IntWritable id;
		private TemporalWritable info;

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			id = new IntWritable(Integer.parseInt(tokens[0]));
			/*
			int originalID = token[1];
			int timestamp = token[2];
			int toOrignalId = token[tokens.length-1];
			int originalTime = 1000000000;
			int vis = 0;
			*/
			info = new TemporalWritable(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), Integer.parseInt(tokens[tokens.length-1]),1000000000, 0);
			return tokens;
		}

		@Override
		protected IntWritable getId(String[] tokens) throws IOException {
			return id;
		}

		@Override
		protected TemporalWritable getValue(String[] tokens) throws IOException {
			return info;
		}

		@Override
		protected Iterable<Edge<IntWritable, IntWritable>> getEdges(
				String[] tokens) throws IOException {
			List<Edge<IntWritable, IntWritable>> edges = Lists
					.newArrayListWithCapacity(Integer.parseInt(tokens[3]));
			for (int n = 0; n < Integer.parseInt(tokens[3]); n ++) {
				edges.add(EdgeFactory.create(
					new IntWritable(Integer.parseInt(tokens[4+n*2])),
					new IntWritable(Integer.parseInt(tokens[4+n*2+1])))
					);
			}
			return edges;
		}
	}
}
