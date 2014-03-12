# Code examples

In the `src/main/java` and `src/main/scala` sub-directories here, we've included a few examples to help you get started.

## Building
In the main project folder type `./sbt` to enter sbt interactive mode. Move to examples using
`project cassovary-examples`. Now by typing `run` you can see and choose an example you would like to run.
You can also use `run ExampleClassName parameters` to run particular example.

## List of examples
* HelloGraph - a "Hello World" example - creates a complete directed graph with 100 nodes and prints out the number of
edges in the graph.
* HelloLoadGraph - load in a graph from two adjacency list text files
* RandomWalk [numSteps] - perform a random walk with `numsteps` steps on a random graph
* RenumberedGraph [numNodes] - performs graph nodes renumbering and prints information about memory gains
* WriteRandomGraph [numNodes] - writes random graph to file in `java.io.tmpdir` jvm property
