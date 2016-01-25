# Cassovary-collections

This subproject contains specialized collections that are used by Cassovary library.

General goals for the collections implemented in this subproject are:

1) low memory footprint (storing primitive types where possible)
2) optimization for Int and Long types (no boxing) (see `CSeqVsSeq` benchmark in 
`cassovary-benchmarks`)
3) generic implementation and ease of conversion to Scala's collections.
