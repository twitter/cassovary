# Cassovary
Cassovary is a simple "big graph" processing library for the JVM.
Most JVM-hosted graph libraries are flexible but not
space efficient. Cassovary is designed from the ground up to first be
able to efficiently handle graphs with billions of nodes
and edges. A typical example usage is to do large scale
graph mining and analysis of a <a href="http://twitter.com">big network</a>.
Cassovary is written in Scala and can be used with any JVM-hosted language.
It comes with some common data structures and algorithms.

Please follow the cassovary project on twitter at [@cassovary](http://twitter.com/cassovary)
for updates.

## Quick Start and Examples
See examples/ for some simple examples of using the library.

## Building
1. ```./sbt update``` (might take a couple of minutes)
2. ```./sbt test```
3. ```sbt package-dist```

## Alternative for using for local projects
1. ```./sbt publish-local```
2. ```cd ../<dependant project>```
3. ```sbt update```

## Comparison to Other Graph Libraries
There are many excellent graph mining libraries already in existence. Most of
them have one or more of the following characteristics:

1. Written in C/C++. Examples include [SNAP](http://snap.stanford.edu/) from Stanford and
[GraphLab](http://graphlab.org/) from CMU. The typical way to use these from JVM is to use
JNI bridges.
2. Sacrifice storage efficiency for flexibility. Examples include
[JUNG](http://jung.sourceforge.net/),
which is written in Java but stores nodes and edges as big objects.
3. Are meant to do much more, typically a full graph database. Examples include
[Neo4J](http://neo4j.org).

On the other hand, Cassovary is intended to be easy to use and extend in a JVM-hosted
environment and yet be efficient enough to scale to billions of nodes and edges.
It is deliberately not designed to provide any persistence or database functionality.

## Mailing list
http://groups.google.com/group/cassovary

Please follow the cassovary project on twitter at [@cassovary](http://twitter.com/cassovary)
for updates.

## Bugs
Please report any bugs to: <https://github.com/twitter/cassovary/issues>

## Authors:
* [Pankaj Gupta](http://twitter.com/pankaj)
* [Dong Wang](http://twitter.com/dongwang218)
* [Tao Tao](http://twitter.com/tao)
* [John Sirois](http://twitter.com/johnsirois)
* [Aneesh Sharma](http://twitter.com/aneeshs)
* [Ashish Goel](http://twitter.com/ashishgoel)
* [Mengqiu Wang](http://twitter.com/4ad)

## License
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
