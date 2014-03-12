# Code examples

In the `src/main/java` and `src/main/scala` sub-directories here, we've included a few examples to help you get started.

## Building using SBT
In the main project folder type `./sbt` to enter sbt interactive mode. Move to examples using
`project cassovary-examples`. Now by typing `run` you can see and choose an example you would like to run.
You can also use `run ExampleClassName parameters` to run particular example.

## Building using Scalac
In the `scala` folder, use `build_run_example.sh` to run the examples. For example,
`examples/src/main/scala/build_run_example.sh` will run the HelloGraph example by default. Other examples
can be run using `examples/scala/build_run_example.sh RandomWalk 10000`.

## Building using Javac
The `java` directory contains most of the same examples, but written in Java. See `build_run_example.sh`
in that directory to build HelloGraph.java. As with the Scala veresion, you can also run other examples like
`examples/src/main/java/build_run_example.sh RandomWalk 10000`. As Cassovary was designed primarily for Scala users,
the Java API is less polished. Improvements on the Java interfaces are a work in progress.

## List of examples
* HelloGraph - a "Hello World" example - creates a complete directed graph with 100 nodes and prints out the number of
edges in the graph.
* HelloLoadGraph - load in a graph from two adjacency list text files
* RandomWalk [numSteps] - perform a random walk with `numsteps` steps on a random graph
* RenumberedGraph [numNodes] - performs graph nodes renumbering and prints information about memory gains
* WriteRandomGraph [numNodes] - writes random graph to file in `java.io.tmpdir` jvm property
