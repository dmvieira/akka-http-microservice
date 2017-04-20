import akka.actor._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import akka.event.{LoggingAdapter, Logging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.IOException
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.math._
import spray.json.DefaultJsonProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Contexts {
  val hash = scala.collection.mutable.HashMap.empty[String, SparkContext]
}

class ContextActor extends Actor {
  def get(name: String) = {
    val conf = new SparkConf()
             .setMaster("local")
             .setAppName(name)
             .set("spark.driver.allowMultipleContexts", "true")
    Contexts.hash.getOrElseUpdate(name, new SparkContext(conf))
  }
  def receive = {
    case name: String => sender ! get(name)
  }
}

object SqlQuery {
  def apply(query: String) = {
    val system = ActorSystem("test")
    implicit val timeout = Timeout(5 seconds)
    val contextActor = system.actorOf(Props[ContextActor], name = "contextActor")
    val future = ask(contextActor, query).mapTo[SparkContext]
    val sc = Await.result(future, timeout.duration).asInstanceOf[SparkContext]
    new SqlQuery(query, sc.uiWebUrl)
  }
}

case class SqlQuery(query: String, sparkUi: Option[String])

trait Protocols extends DefaultJsonProtocol {
  implicit val sqlQueryFormat = jsonFormat2(SqlQuery.apply)
}

trait Service extends Protocols {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: Materializer

  def config: Config
  val logger: LoggingAdapter

  val routes = {
    logRequestResult("akka-http-microservice") {
      pathPrefix("query") {
        (get & path(Segment)) { query =>
          complete {
            SqlQuery(query)
          }
        } ~
        (post & entity(as[SqlQuery])) { sqlQuery =>
          complete(sqlQuery)
        }
      }
    }
  }
}

object AkkaHttpMicroservice extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  override val config = ConfigFactory.load()
  override val logger = Logging(system, getClass)

  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))
}
