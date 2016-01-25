# Code for benchmarking

Some example graphs have been put in `cassovary-benchmarks/src/main/resources` on which
benchmarks included in `cassovary-benchmarks/src/main/scala` can be run.

## Building and running - traditional benchmarks
In the main project folder type `./sbt` to enter sbt interactive mode. Move to
cassovary-benchmarks project using
`project cassovary-benchmarks`. Now by typing `run` you can see and choose one
of the benchmarks you would like to run. You can also
use `runMain BenchmarkClassName parameters` to run a particular benchmark,
for example `runMain com.twitter.cassovary.PerformanceBenchmark -local=facebook -globalpr`

## Building and running - JMH benchmarks

JMH is a JVM benchmarking tool that makes sure that your code is tested after JIT
optimizations. In Cassovary JMH is provided by an SBT plugin. 

To run the benchmark type `./sbt` and `project cassovary-benchmarks` followed by
JMH run command: `jmh:run -i 3 -wi 3 .*CSeqVsSeq.*`. This command will run 3 iterations
proceeded by 3 warmup iterations defined in file that matches regexp at the end. 

For more details on sbt-jmh we recommend [[https://github.com/ktoso/sbt-jmh]].
