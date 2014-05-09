/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.graph2

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import com.twitter.cassovary.util.{ByteBufferBackedLongSeq, ConstantDB}
import com.twitter.util.Future

/**
 * Graph based on an underlying ConstantDB backed key-value store. The keys are
 * of type Long and values are of type Seq[Long]
 */
class SimpleMappedGraph(
  graphDir: String
) extends SimpleGraph2 {
  import SimpleMappedGraph._

  val cdb = new ConstantDB(graphDir, keySerializer _, keyDeserializer _, valueDeserializer _)

  override def neighbors(u: Long): Future[Option[Seq[Long]]] = Future.value(cdb(u))
}

object SimpleMappedGraph {
  private def keySerializer(k: Long) = {
    val bb = ByteBuffer.allocate(8) 
    bb.putLong(k)
    bb.flip()
    bb
  }

  private def keyDeserializer(bb: ByteBuffer) = bb.getLong()

  private def valueDeserializer(bb: ByteBuffer) = new ByteBufferBackedLongSeq(bb)

  private def valueSerializer(vs: Seq[Long]) = {
    val bb = ByteBuffer.allocate(vs.size * 8)
    vs foreach { v => bb.putLong(v) }
    bb.flip()
    bb
  }

  def collectEdges[T](input: Stream[(T, T)]): Stream[(T, Seq[T])] = {
    if (input.isEmpty) {
      Stream.empty
    } else {
      val (source, dest) = input.head
      val xs = input.tail
      val (sibs, rest) = xs span { case (s, _) => s == source }
      (source, (dest +: (sibs map { case (_, d) => d } )).toList) #:: collectEdges(rest)
    }
  }

  def write(folder: String, input: Stream[(Long, Seq[Long])]) {
    val binaryData = input map { case (k, v) => (keySerializer(k), valueSerializer(v)) }
    ConstantDB.write(folder, binaryData)
  }
}

