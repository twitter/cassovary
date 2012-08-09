import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.cassovary.server.{CachedDirectedGraphServer, CachedDirectedGraphServerConfig, LogStats}
import com.twitter.logging.config.{FileHandlerConfig, LoggerConfig, ConsoleHandlerConfig, Level}
import com.twitter.ostrich.admin.ServiceTracker
import com.twitter.logging.{Logger, Policy}

object RunCDGServer {
  val log = Logger.get(getClass.getName)

  def main(args: Array[String]) {

    val runtime = RuntimeEnvironment(this, args)
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
