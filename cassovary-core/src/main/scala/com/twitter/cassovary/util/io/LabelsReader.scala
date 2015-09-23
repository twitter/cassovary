package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph.labels._
import com.twitter.cassovary.util.ParseString
import com.twitter.util.NonFatal
import java.io.IOException
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Only reads node labels where the key is of type int.
 * Label values can be of type int and string.
 *
 * ASSUMES that the label files are named as follows:
 * `collPrefix`_anything_labelName_labelValueType.txt
 *
 * So the file name starts with an identifier that marks this collection of labels to be read.
 * And that each line has an id followed by a single space followed by int value of label
 * <id> <labelValue>
 */
class LabelsReader(val directory: String, val collPrefix: String, val separator: Char = ' ') {
  val stringType = typeTag[String]
  val intType = typeTag[Int]

  /*
    ClassTag is required to be able to build an array of type L
    TypeTag is required to be able to retrieve values of type L from the Label
   */
  def readOneLabel[L : ClassTag: TypeTag](labelName: String, fileName: String,
      isSparse: Option[Boolean] = None, maxId: Option[Int] = None): Label[Int, L] = {
    val tag = implicitly[TypeTag[L]]
    if (tag != intType && tag != stringType) {
      throw new Exception("Only Int and String label values are supported right now!")
    }

    val label = (isSparse, maxId) match {
      case (Some(false), Some(m)) => new ArrayBasedLabel[L](labelName, m)
      case _ => new IntLabel[L](labelName, maxId)
    }
    var lastLineParsed = 0
    val source = Source.fromFile(fileName)
    source.getLines().foreach { line =>
      lastLineParsed += 1
      try {
        val i = line.indexOf(separator)
        val id = ParseString.toInt(line, 0, i - 1)
        val labelValue = if (tag == intType) {
          ParseString.toInt(line, i + 1, line.length - 1)
        } else if (tag == stringType) {
          line.substring(i + 1, line.length)
        }
        label.update(id, labelValue.asInstanceOf[L])
      } catch {
        case NonFatal(exc) =>
          throw new IOException("Parsing failed near line: %d in %s"
              .format(lastLineParsed, fileName), exc)
      }
    }
    source.close()
    label
  }

  /**
   * @param isSparse when false and a maxId is provided, an array-based label is allocated
   *                 else a map based label is used
   * @param maxId when maximum value of ID is known, it can be used to estimate the sizing of
   *              internal data structures
   */
  def read(isSparse: Option[Boolean] = None, maxId: Option[Int] = None): Labels[Int] = {
    def parse(fileName: String) : (String, String) = {
      val fileNameParts = fileName.split(Array('_', '.'))
      val name = fileNameParts(fileNameParts.length - 3)
      val valueType = fileNameParts(fileNameParts.length - 2)
      (name, valueType)
    }

    val labelFiles = IoUtils.readFileNames(directory, collPrefix)
    val labels = new Labels[Int]
    labelFiles foreach { fileName =>
      val (labelName, labelTypeString) = parse(fileName)
      labelTypeString.toLowerCase match {
        case "int" => labels += readOneLabel[Int](labelName, fileName, isSparse, maxId)
        case "string" => labels += readOneLabel[String](labelName, fileName, isSparse, maxId)
        case _ => println(s"unable to understand label value type of $fileName. Ignoring this file.")
      }
    }
    labels
  }

}
