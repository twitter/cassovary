import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.cassovary.server.{CachedDirectedGraphServer, CachedDirectedGraphServerConfig, LogStats}
import com.twitter.logging.config.{FileHandlerConfig, LoggerConfig, ConsoleHandlerConfig, Level}
import com.twitter.ostrich.admin.ServiceTracker
import com.twitter.logging.{Logger, Policy}
import net.liftweb.json

object RunCDGServer {
  val log = Logger.get(getClass.getName)

  def main(args: Array[String]) {

    if (args.length < 1) {
      throw new Exception("Provide JSON config file as the first argument!")
    }

    val runtime = RuntimeEnvironment(this, args.drop(1))
    val server = if (runtime.configFile.exists) {
      runtime.loadRuntimeConfig[CachedDirectedGraphServer]
    } else {
      (new CachedDirectedGraphServerConfig {

        // Basic configuration
        serverPort = 9999
        admin.httpPort = 9900
        loggers = new LoggerConfig {
          level = Level.INFO
          handlers = new FileHandlerConfig {
            filename = "/tmp/production.log"
            roll = Policy.SigHup
          }
        }

        // Start admin stats service
        admin()(runtime)

        // Start stats logging service
        val deltaStats = new LogStats(10, false)
        val aggStats = new LogStats(60, true)
        ServiceTracker.register(deltaStats)
        ServiceTracker.register(aggStats)
        deltaStats.start()
        aggStats.start()

        // Load params
        implicit val formats = net.liftweb.json.DefaultFormats
        val sData = (json.parse(scala.io.Source.fromFile(args(0)).mkString) \ "server")
        nodeList = (sData \ "nodelist").extract[String]
        verbose = (sData \ "verbose").extract[Boolean]
        graphDump = (sData \ "graph_dump").extract[String]
        cacheType = (sData \ "cache_type").extract[String]
        numNodes = (sData \ "num_nodes").extract[Int]
        numEdges = (sData \ "num_edges").extract[Long]
        shardDirectories = (sData \ "shard_directories").extract[String].split(":")
        numShards = (sData \ "num_shards").extract[Int]
        numRounds = (sData \ "num_rounds").extract[Int]
        cacheDirectory = (sData \ "cache_directory").extract[String]
        experiment = (sData \ "experiment").extract[String]
        iterations = (sData \ "iterations").extract[Int]
        outputDirectory = (sData \ "output_directory").extract[String]
      })(runtime)
    }

    val loggers = List(
      new LoggerConfig {
        level = Level.INFO
        handlers = new FileHandlerConfig {
          filename = "/tmp/production.log"
          roll = Policy.SigHup
        }
      }
    )
//    ,
//    new LoggerConfig {
//      level = Level.INFO
//      handlers = new ConsoleHandlerConfig()
//    }
    Logger.configure(loggers)

    try {
      server.start()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        log.error(e, "Unexpected exception: %s", e.getMessage)
        System.exit(0)
    }
  }
}
