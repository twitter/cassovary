import com.twitter.cassovary.util.{FileUtils, LoaderSerializerReader, Renumberer}
import java.io.File

object RenumbererMapper {
  def main(args: Array[String]) {

    var renumberer: Renumberer = null
    if (args.size > 0) {
      try {
        renumberer = new Renumberer(args(0).toInt)
      }
      catch {
        case e: Exception =>
          renumberer = new Renumberer(1)
          renumberer.fromReader(new LoaderSerializerReader(args(0)))
      }
    }

    // RenumbererMapper path/to/mapping path/to/inputIds path/to/outputIndices
    if (args.size > 2) {
      println("Reading ids from %s and writing forward mappings to %s...".format(args(1), args(2)))
      FileUtils.readLinesAndPrintToFile(args(1), args(2)) { (l, p) =>
        p.println(renumberer.translate(l.toInt))
      }
    }

    println("f = forward map, r = reverse map")

    while (true) {
      print("Type in [f/r][number]: ")
      val in = readLine()

      try {
        val num = in.substring(1).stripLineEnd.toInt
        val dir = in.substring(0, 1)

        dir match {
          case "r" => println("%d <- %d".format(num, renumberer.reverseTranslate(num)))
          case "f" => println("%d -> %d".format(num, renumberer.translate(num)))
        }

      }
      catch {
        case e: Exception => println("Invalid Input! Try again?")
      }
    }

  }
}
