import com.twitter.cassovary.util.{LoaderSerializerReader, Renumberer}

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
