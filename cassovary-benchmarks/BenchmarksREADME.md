# Code for benchmarking

Some example graphs have been put in `cassovary-benchmarks/src/main/resources` on which
benchmarks included in `cassovary-benchmarks/src/main/scala` can be run.

## Building and running
In the main project folder type `./sbt` to enter sbt interactive mode. Move to
cassovary-benchmarks project using
`project cassovary-benchmarks`. Now by typing `run` you can see and choose one
of the benchmarks you would like to run. You can also
use `runMain BenchmarkClassName parameters` to run a particular benchmark,
for example `runMain com.twitter.cassovary.PerformanceBenchmark -local=facebook -globalpr`
