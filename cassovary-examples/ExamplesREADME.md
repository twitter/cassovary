# Code examples

In the `cassovary-examples/src/main/java` and `cassovary-examples/src/main/scala` sub-directories here,
we have included a few examples to help you get started and illustrate some example usage of the
library.

## Building
In the main project folder type `./sbt` to enter sbt interactive mode. Move to examples project using
`project cassovary-examples`. Now by typing `run` you can see and choose an example you would like to run.
You can also use `runMain ExampleClassName parameters` to run a particular example,
for example `runMain HelloGraph 10`

Alternatively, on the command line type ```bash cassovary-examples/src/main/load-graph-examples.sh``` to
generate a random graph with 5M nodes and approximately 100M edges and then load it.

## Some included examples
* HelloGraph [numNodes] - a "Hello World" example - creates a complete directed graph with 3 nodes and prints out the number of
edges in the graph.
* HelloLoadGraph - load in a graph from two adjacency list text files in resources/
* RandomWalk [numSteps] - perform a random walk with `numsteps` steps on a random graph
* RenumberedGraph [numNodes] - performs graph nodes renumbering and prints information about memory gains
* WriteRandomGraph [numNodes] - writes random graph to file in `java.io.tmpdir` jvm property
