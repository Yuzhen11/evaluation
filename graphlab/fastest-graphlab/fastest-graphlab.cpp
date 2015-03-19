#include <vector>
#include <string>
#include <fstream>
#include <map>
#include <graphlab.hpp>

const int inf = 1e9;
int SOURCE_VERTEX;
int sourceVertexes[7][10] = {
{6186162, 12301725, 20134680, 20740719, 2342319, 17279331, 16870427, 4515022, 17337467, 8774179},
{2315510, 1891286, 737079, 4301237, 2579159, 1352507, 506412, 4182233, 4067988, 1333920},
{1566590, 264625, 412216, 715250, 1714414, 254775, 821137, 788822, 745114, 79641},
{420039, 1754984, 1672567, 86203, 1402919, 2046159, 468839, 1290343, 992632, 1641554},
{1991042, 2887286, 1399630, 1059254, 1449362, 850708, 2106851, 1964219, 3136747, 1518994},
{961004, 682851, 790755, 847970, 996894, 744495, 406209, 808085, 1091325, 589901},

{0,1,2,3,4,0,1,2,3,4}
};
std::map<std::string, int> dic;

struct vertex_data : graphlab::IS_POD_TYPE
{
    int originalID;
    int timestamp;
    int toOriginalId;

    int arrivalTime;
    int vis;

    vertex_data()
    {

    }
    vertex_data(int t1, int t2, int t3, int t4, int t5):originalID(t1),timestamp(t2),toOriginalId(t3),arrivalTime(t4),vis(t5)
    {}
};

struct edge_data : graphlab::IS_POD_TYPE
{
    int w;
    edge_data()
    {

    }
};
struct max_distance_type : graphlab::IS_POD_TYPE
{
    int dist;
    max_distance_type(int dist = inf) : dist(dist){}
    max_distance_type& operator+=(const max_distance_type& other)
    {
        dist = std::max(dist, other.dist);
        return *this;
    }
};
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

void init_vertex(graph_type::vertex_type& vertex)
{
    vertex.data().arrivalTime = inf;
    vertex.data().vis = -1;//start
}

// gather type is graphlab::empty, then we use message model
class fastest : public graphlab::ivertex_program<graph_type, graphlab::empty,
 max_distance_type>, public graphlab::IS_POD_TYPE
 {
    int max_dist;
    bool changed;
public:

    void init(icontext_type& context, const vertex_type& vertex,
              const max_distance_type& msg)
    {
        max_dist = msg.dist;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const graphlab::empty& empty)
    {
        changed = false;
        if (context.iteration() == 0 )
        {
            if (vertex.data().originalID == SOURCE_VERTEX && vertex.data().timestamp >= 0)
            {
                changed = true;
                vertex.data().vis = vertex.data().timestamp;
            }
        }
        else
        {
            if (vertex.data().timestamp < 0)
            {
                int tmp = -max_dist;
                if (vertex.data().arrivalTime > tmp)
                    vertex.data().arrivalTime = tmp;
                
            }
            else 
            {
            	if (max_dist > vertex.data().vis)
            	{
		        	changed = true;
		        	vertex.data().vis = max_dist;
				}
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (changed)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {
        const vertex_type other = edge.target();
        int newd;
        if (edge.data().w == -1)
            newd = -(vertex.data().timestamp-vertex.data().vis);
        else
            newd = vertex.data().vis;

        const max_distance_type msg(newd);
        context.signal(other, msg);

    }
 };


struct temporal_writer
{
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.data().originalID << "\t" << vtx.data().arrivalTime << "\n";
        if (vtx.data().timestamp < 0)
            return strm.str();
        else
            return "";
    }
    std::string save_edge(graph_type::edge_type e)
    {
        return "";
    }
};
bool line_parser(graph_type& graph, const std::string& filename,
                 const std::string textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int originalID, timestamp, nb, w;
    graphlab::vertex_id_type t, toOriginalId;
    ssin >> originalID >> timestamp >> nb;
    for (int i = 0; i < nb; ++ i)
    {
        edge_data edge;
        ssin >> t >> edge.w;
        graph.add_edge(vid, t, edge);
    }
    ssin >> toOriginalId;
    graph.add_vertex(vid, vertex_data(originalID, timestamp, toOriginalId, inf, 0));
    edge_data edge;
    edge.w = -1;
    graph.add_edge(vid, toOriginalId, edge);

    return true;
}


int main(int argc, char** argv)
{
	dic["edit"] = 0;
	dic["delicious"] = 1;
	dic["wiki"] = 2;
	dic["flickr"] = 3;
	dic["youtube"] = 4;
	dic["dblp"] = 5; 
	dic["toy"] = 6;
	dic["test"] = 7;
    SOURCE_VERTEX = 0;

    graphlab::mpi_tools::init(argc, argv);
    //char *input_file = "hdfs://master:9000/yuzhen/toyT";
    char *input_file = argv[1];
    std::string graph_file = argv[2];
    char *output_file = "hdfs://master:9000/yuzhen/output/";
    std::string exec_type = "synchronous";
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    graphlab::timer t;
    t.start();
    graph_type graph(dc);
    graph.load(input_file, line_parser);
    graph.finalize();
    //load done
    dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;
    
    const int QUERY_SIZE = 10;
    int p = dic[graph_file];
    double t_compute = 0, t_dump = 0;
    for (int i = 0; i < QUERY_SIZE; ++ i)
    {
    	if (p == 7) SOURCE_VERTEX = 0;
    	else SOURCE_VERTEX = sourceVertexes[p][i];
    	dc.cout() << "source vertex: " << SOURCE_VERTEX << std::endl;
    	
		t.start();
		graph.transform_vertices(init_vertex);
		graphlab::omni_engine<fastest> engine(dc, graph, exec_type);
		engine.signal_all();
		engine.start();
		
		t_compute += t.current_time();
		t.start();
		
		std::stringstream srtm;
		srtm << output_file  << i;
		graph.save(srtm.str().c_str(), temporal_writer(), false, // set to true if each output file is to be gzipped
		           true, // whether vertices are saved
		           false); // whether edges are saved
		           
		t_dump += t.current_time();
	}
	dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
	dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;

    graphlab::mpi_tools::finalize();
}

