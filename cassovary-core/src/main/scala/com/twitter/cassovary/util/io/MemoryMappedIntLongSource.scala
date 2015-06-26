/*
 * Copyright 2015 Teapot, Inc.
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
package com.twitter.cassovary.util.io

import java.io.File
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.nio.MappedByteBuffer

/**
 * Represents an arbitrarily large sequence of bytes which can be interpreted as ints or longs.
 */
trait IntLongSource {
  /** Returns the int at the given byte index, which must be a multiple of 4. */
  def getInt(index: Long): Int
  /** Returns the Long at the given byte index, which must be a multiple of 8. */
  def getLong(index: Long): Long
}

/**
 * Wraps a sequence of FileChannels to enable random access on a memory mapped file of arbitrary size.
 * Motivation: FileChannel.open only supports 2GB at a time.
 * @param file The file containing binary data.
 */

class MemoryMappedIntLongSource(file: File) extends IntLongSource {
  def this(fileName: String) = this(new File(fileName))

  private val fileChannel = FileChannel.open(file.toPath, StandardOpenOption.READ)

  // Each buffer uses int addressing, so can only access 2GB.  We'll use multiple buffers,
  // and access 2^30 bytes per buffer.
  // We use 2^30 bytes per buffer rather than 2^31 because fileChannel.map doesn't currently
  // support size 2^31.
  private val bytesPerBuffer = 1L << 30
  // ceiling of file.length() / bytesPerBuffer
  private val bufferCount = ((file.length() + bytesPerBuffer - 1) / bytesPerBuffer).toInt
  private val byteBuffers: Array[MappedByteBuffer] = (0 until bufferCount).toArray map { bufferIndex =>
    val size = if (bufferIndex + 1 < bufferCount)
      bytesPerBuffer
    else
      file.length - (bufferCount - 1L) * bytesPerBuffer
    fileChannel.map(MapMode.READ_ONLY, bufferIndex * bytesPerBuffer, size)
  }
  private def bufferIndex(index: Long): Int = (index >> 30).toInt
  private def indexWithinBuffer(index: Long): Int = (index & 0x3FFFFFFF).toInt

  def getInt(index: Long): Int = byteBuffers(bufferIndex(index)).getInt(indexWithinBuffer(index))

  def getLong(index: Long): Long = byteBuffers(bufferIndex(index)).getLong(indexWithinBuffer(index))
}
