/**
 * Singleton for processing platform generic viewership data into Snapshot objects.
 * @author Jeremy Owens
 * @version 1.0
 * 
 */


package tv.tsagh.datacollection

import play.api.libs.json._
import javax.net.ssl._
import scala.io.Source
import java.security.cert.X509Certificate
import scala.io.Source

//Case class wrapper for stream information
case class Snapshot(t: java.sql.Timestamp, c: String, g: String, v: Long, f: Long, p: String)

/* Wrapper for platform specific functions. This is dependant on the API schema */
case class PlatformFunctions(p: String, 
                             url: String, //The general path for GET requests. It should end with the pagination parameter
                             inc: (=> String, => String) => String, //Function to get the next pagination token
                             init: String => Seq[JsValue], //Function for building the list of json object                          
                             v: JsValue => Long, //Function for retrieving the number of viewers
                             f: JsValue => Long, //Function for retrieving the number of followers or subscribers
                             c: JsValue => String, //Function for retrieving the channel name or id
                             g: JsValue => String, //Function for retrieving the game being played, or type of broadcast
                             filt: JsValue => Boolean, //Function for filtering results. For no filter, just set TRUE
                             arr: Array[(String,String)]) //Array of tuples representing key/value pairs to be passed as request properties

object BroadcastDataProcessor {
  
  /**
   * Recursive, tail-safe function for iterating through results.
   * 
   * @param l    List[Snapshot]     this is the list that output will be combined with.
   * @param ts   java.sql.Timestamp the time that the data pull sequence was initiated.
   * @param fC   Int                the number of times the attempt may fail before results are returned incomplete
   * 
   */
  @annotation.tailrec
  def buildWhile(l: List[Snapshot], ts: java.sql.Timestamp, fC: Int, o: String, pf: PlatformFunctions): List[Snapshot] = 
    if (fC < 1) l //If the process has failed enough too many times, return the list as is
    else get(pf.url + o, pf.arr) match {
        case None => buildWhile(l, ts, fC - 1, o, pf) //If none, decrement the fail counter and try again
        case Some(a) => { //If Some value, pass into snapshotlist along with the platformfunctions
          val inc = pf.inc(o,a)
          val next = snapshotList(Nil, pf.init(a:String), ts, pf)
          if (next == Nil || inc == o) l ++ next
          else buildWhile(l ++ next, ts, fC, inc, pf)
          }
        }
  
  /**
   *  Recursive tail-safe function for converting a Sequence of JsValues into a List of Snapshots 
   *  
   *  @param l    List[Snapshot]     A list of snapshots, either Nil or from the previous get request
   *  @param xs   Seq[JsValue]       A sequence of Play Json objects created using
   *  @param t    java.sql.Timestamp the time that the data pull sequence was initiated.
   *  @param pf   PlatformFunctions  The 
   */
  @annotation.tailrec
  def snapshotList(l: List[Snapshot], xs: Seq[JsValue], t: java.sql.Timestamp, pf: PlatformFunctions): List[Snapshot] =
    xs match {
      case Nil => l
      case head::tail => {
        //If result does not meet the filter trequirements, process the tail without adding a new element to l
        if (!pf.filt(head)) snapshotList(l, tail, t, pf)
        else {
          l match {
            case Nil => snapshotList(
                List(Snapshot(t, pf.c(head), pf.g(head), pf.v(head), pf.f(head), pf.p.toString())),
                tail, t, pf) //If there is no list, create a list with the first element
            //If a list exists, add it as a tail where the new element is the head, and process the tail
            case _ => snapshotList(Snapshot(t, pf.c(head), pf.g(head), pf.v(head), pf.f(head), pf.p.toString()):: l, tail, t, pf)
          }
        }
      }
    }
  
  /**
   *  Function to ensure safety of Source.fromUrl and apply request properties optionally 
   *  
   *  @param url   String                    A formatted url string
   *  @param args  Array[(String,String)]    String tuples to be defined as key/value pairs in request properties
   */
  def get(url: String, args: Array[(String,String)]): Option[String] = {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.setRequestMethod("GET")
    for ((k,v) <- args) {
      connection.setRequestProperty(k,v)
    }
    try {
      val code: Int = connection.getResponseCode
      if (code != 200) None
      else {
        val inputStream = connection.getInputStream
        val content = Source.fromInputStream(inputStream, "UTF-8").mkString
        if (inputStream != null) inputStream.close
        Some(content)
        }      
      }
    catch {case e:Exception => None}
  }
  
  //Setting to ignore bad SSL certs
  val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
    HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)

  /* Objects to force trusting certificates */
  object TrustAll extends X509TrustManager {
    val getAcceptedIssuers = null
    def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
    def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
  }
  /* Declaring safety to be true */
  object VerifiesAllHostNames extends HostnameVerifier {
    def verify(s: String, sslSession: SSLSession) = true
  }
}