package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph.DirectedGraph

/**
 * Created by bmckown on 1/28/15.
 */
trait Centrality {
  def run: Array[Double]
}
