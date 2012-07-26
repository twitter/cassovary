package com.twitter.cassovary.util

trait IntArrayCache {
  def get(id: Int):Array[Int]
}

class FastLRUIntArrayCache(shardDirectory: String, numShards: Int,
                            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                            idToIntOffsetAndNumEdges:Array[(Long,Int)]) extends IntArrayCache {

  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val emptyArray = new Array[Int](0)
  val idToArray = new Array[Array[Int]](maxId+1)
  val linkedMap = new LinkedIntIntMap(maxId, cacheMaxNodes)
  var currRealCapacity: Long = 0
  var hits, misses: Long = 0

  // TODO Synchronize
  def get(id: Int):Array[Int] = {
    if (linkedMap.contains(id)) {
      hits += 1
      linkedMap.moveToHead(id)
      idToArray(id)
    }
    else {
      misses += 1
      idToIntOffsetAndNumEdges(id) match {
        case (offset, numEdges) => {
          // Read in array
          val intArray = new Array[Int](numEdges)
          reader.readIntegersFromOffsetIntoArray(id, offset * 4, numEdges, intArray, 0)

          // Evict from cache
          currRealCapacity += numEdges
          while(linkedMap.getCurrentSize == cacheMaxNodes || currRealCapacity > cacheMaxEdges) {
            currRealCapacity -= idToArray(linkedMap.getTailId).length
            linkedMap.removeFromTail()
          }

          linkedMap.addToHead(id)
          idToArray(id) = intArray
          intArray
        }
        case null => throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
    }
  }

  def getStats = {
    (misses, hits, linkedMap.getCurrentSize, currRealCapacity)
  }

}
