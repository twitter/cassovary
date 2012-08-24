# Code examples

In the `java` and `scala` sub-directories here, we've included a few examples to help you get started.

## Building using Scala
In the `scala` folder, use `build_run_example.sh` to run the examples. For example,
`examples/scala/build_run_example.sh` will run the HelloGraph example by default. Other examples
can be run using `examples/scala/build_run_example.sh RandomWalk 10000`.

## Building using Java
The `java` directory contains most of the same examples, but written in Java. See `build_run_example.sh`
in that directory to build HelloGraph.java. As with the Scala veresion, you can also run other examples like
`examples/java/build_run_example.sh RandomWalk 10000`. As Cassovary was designed primarily for Scala users,
the Java API is less polished. Improvements on the Java interfaces are a work in progress.

## List of examples
* HelloGraph - a "Hello World" example - creates a complete directed graph with 100 nodes and prints out the number of
edges in the graph.
* HelloLoadGraph - load in a graph from two adjacency list text files
* RandomWalk [numSteps] - perform a random walk with `numsteps` steps on a random graph