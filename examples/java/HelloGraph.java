import com.twitter.cassovary.graph.DirectedGraph;
import com.twitter.cassovary.graph.TestGraph;
import com.twitter.cassovary.graph.TestGraphs;

public class HelloGraph {
	public static void main(String[] args){
		int numNodes = 3;
		if (args.length > 0) numNodes = Integer.parseInt(args[0]);
		System.out.printf("Generating a complete directed graph with %d nodes...\n", numNodes);
		DirectedGraph graph = TestGraphs.generateCompleteGraph(numNodes);
		System.out.printf("Hello Graph!\n\tA complete directed graph with %s nodes has %s directed edges.\n", graph.nodeCount(), graph.edgeCount());
	}
}