/**
 * This is a singleton object defining the functions   
 * necessary to process data from 5 different sources. 
 * Note that the paths may change at any time. These are
 * only examples and may not work for your use case. It 
 * is recommended to define your own PlatformFunctions 
 * that fit your own specifications.
 *                                                     
 * @author Jeremy Owens
 * @version 1.0
*/

package tv.tsagh.datacollection

import play.api.libs.json._
import com.typesafe.config.ConfigFactory

object PlatformFunctionDefinitions {
   val conf = ConfigFactory.load()
  
   val youtubePFuncs = PlatformFunctions (
       "YouTube",
       "https://www.googleapis.com/youtube/v3/playlistItems?key=" + conf.getString("YOUTUBE_API_KEY") + "&part=contentDetails&playlistId=PLiCvVJzBupKmEehQ3hnNbbfBjLUyvGlqx&maxResults=50&pageToken=",
      // "https://www.googleapis.com/youtube/v3/search?key=" + System.getenv("YOUTUBE_API_KEY") + "&part=snippet&eventType=live&type=video&videoCategoryId=20&maxResults=50&pageToken=",
       (i, a) => (safeParse(a).getOrElse(JsNull) \ "nextPageToken").asOpt[String].getOrElse(i),
       a => ytInit(a).getOrElse(Nil),
       a => (a \ "video" \ "viewers").asOpt[String].getOrElse("0").toLong,
       a => (a \ "channel" \ "statistics" \ "subscriberCount").asOpt[String].getOrElse("0").toLong,
       a => (a \ "channel" \ "id").asOpt[String].getOrElse(""),
       a => (a \ "title").asOpt[String].getOrElse(""), 
       a => (a \ "channel" \ "statistics" \ "subscriberCount").asOpt[String].getOrElse("0").toLong >= 500,
       Array()
       )
       
   val abuzuPFuncs = PlatformFunctions (
        "Abuzu",
        "http://api.azubu.tv/public/channel/live/list?limit=100&offset=",
        (i, _) => (i.toInt + 90).toString,
        a => (safeParse(a).getOrElse(JsNull) \ "data").asOpt[Seq[JsValue]].getOrElse(Nil),
        a => (a \ "view_count").asOpt[Long].getOrElse(0.toLong),  
        a => (a \ "followers_count").asOpt[Long].getOrElse(0.toLong),
        a => (a \ "user" \ "username").asOpt[String].getOrElse("").toLowerCase,
        a => (a \ "category" \ "title").asOpt[String].getOrElse(""),
        a => (a \ "followers_count").asOpt[Long].getOrElse(0.toLong) >= 500,
        Array()
        )
        
   val twitchPFuncs = PlatformFunctions(
        "Twitch",
        "https://api.twitch.tv/kraken/streams?limit=100&offset=",
        (i, _) => (i.toInt + 90).toString,
        a => (safeParse(a).getOrElse(JsNull) \ "streams").asOpt[Seq[JsValue]].getOrElse(Nil),
        a => (a \ "viewers").asOpt[Long].getOrElse(0.toLong), 
        a => (a \ "channel" \ "followers").asOpt[Long].getOrElse(0.toLong), 
        a => (a \ "channel" \ "name").asOpt[String].getOrElse(""), 
        a => (a \ "game").asOpt[String].getOrElse(""), 
        //Accept English speaking channels with at least 500 followers
        a => (a \ "channel" \ "followers").asOpt[Long].getOrElse(0.toLong) >= 500 && (a \ "channel" \ "broadcaster_language").asOpt[String].getOrElse("") == "en",
        Array(("Accept", "application/vnd.twitchtv.v3+json"),("Client-ID", conf.getString("TWITCH_API_KEY")))
        )
        
    val beamPFuncs = PlatformFunctions(
        "Beam",
        "https://beam.pro/api/v1/channels?order=viewersCurrent:DESC&where=viewersCurrent:gte:10,numFollowers:gte:1000&fields=userId,token,viewersCurrent,numFollowers,type&page=",
        (i, _) => (i.toInt + 1).toString,
        a => safeParse(a).getOrElse(JsNull).asOpt[Seq[JsValue]].getOrElse(Nil),
        a => (a \ "viewersCurrent").asOpt[Long].getOrElse(0.toLong),
        a => (a \ "numFollowers").asOpt[Long].getOrElse(0.toLong),
        a => (a \ "token").asOpt[String].getOrElse(""),
        a => {
          val tp = (a \ "type").asOpt[JsValue].getOrElse("")
          if (tp.isInstanceOf[JsValue]) {
            val tpj: JsValue = tp.asInstanceOf[JsValue]
            (tpj \ "name").asOpt[String].getOrElse("")
          } else ""
        },
        // The following function sets the filter to save data for any channel with 500 or more followers
        a => {
          val tp = (a \ "type").asOpt[JsValue].getOrElse("")
          val game = if (tp.isInstanceOf[JsValue]) {
            val tpj: JsValue = tp.asInstanceOf[JsValue]
            (tpj \ "name").asOpt[String].getOrElse("")
          } else ""
          
          (a \ "numFollowers").as[Long] >= 500
        },
        Array()
        )
        
    val hitboxPFuncs = PlatformFunctions(
        "HitBox",
        "https://www.hitbox.tv/api/media/live/list?liveonly=true&limit=100&offset=",
        (i, _) => (i.toInt + 90).toString,
        a => (safeParse(a).getOrElse(JsNull) \ "livestream").asOpt[Seq[JsValue]].getOrElse(Nil),
        a => (a \ "media_views").asOpt[String].getOrElse("0").toLong,
        a => (a \ "channel" \ "followers").asOpt[String].getOrElse("0").toLong,
        a => (a \ "media_display_name").asOpt[String].getOrElse(""),
        a => (a \ "category_name").asOpt[String].getOrElse(""),
        // The following function sets the filter to save data for any channel with 1000 or more followers and either speaks
        // English or has language undefined. HitBox is a bit of a mess here, since the field allows null, multiples, and mixed
        // case entries (but stores them case sensitive).
        a => {
          val ct = (a \ "media_countries").asOpt[Array[String]].getOrElse(Array(""))(0)
          ((a \ "channel" \ "followers").asOpt[String].getOrElse("0").toLong >= 500 && (ct == "" || ct == "en" || ct == "EN"))
        },
        Array()
        )
        
  //Function to lift Json.parse to Option      
  def safeParse(js: String): Option[JsValue] = {
     try {
       val x = Json.parse(js)
       Some(x)
     }
     catch {
       case e: Exception => None
     }
   } 
   
  /*This is a really long function to force the YouTube API to work like everyone else's*/
  def ytInit(page: String): Option[Seq[JsValue]] = vidList(vidIds(page))
  
  @annotation.tailrec
  def getObj(id: String, s: Seq[JsValue]) : JsValue = 
    s match {
      case Nil => JsString("")
      case h::t => if ((h \ "id").asOpt[String].getOrElse("") == id) h else getObj(id,t)
    }
                
  @annotation.tailrec
  def getGameName(id: String, gameSet: List[(String,JsString)]): JsString = 
    gameSet.toList match {
      case Nil => JsString("")
      case h::t => if (h._1 == id) h._2 else getGameName(id,t)
    }
  
  def vidIds(p: String): Option[String] = {
      val vIds = java.net.URLEncoder.encode(
        (safeParse(p).getOrElse(JsNull) \ "items").asOpt[Seq[JsValue]].getOrElse(Nil).
         map(a => (a \ "contentDetails" \ "videoId").
         asOpt[String].getOrElse("")).
         foldLeft("")((a,b) => b + "," + a).dropRight(1), "UTF-8")
      if (vIds.length < 1) None
      else Some(vIds)
    }
  
  def chanList(chanIds: String): Option[JsValue] = safeParse(BroadcastDataProcessor.get("https://www.googleapis.com/youtube/v3/channels?part=snippet%2Cstatistics&id=" + 
            chanIds + "&key=" + conf.getString("YOUTUBE_API_KEY"), Array()).getOrElse(""))
            
  def vidList(vIds: Option[String]): Option[Seq[JsValue]] = 
    vIds match {
      case None => None
      case Some(vidIdString) => {
        //Safely compile a Sequence of video items, or Nil
        val vidItems = (
            safeParse(BroadcastDataProcessor.get(
            "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CliveStreamingDetails%2CtopicDetails&fields=items(id%2Csnippet(title%2CchannelId)%2CtopicDetails(topicIds)%2CliveStreamingDetails(concurrentViewers))&id=" +
            vidIdString + "&key=" + conf.getString("YOUTUBE_API_KEY"), Array()).getOrElse("")).getOrElse(JsNull) \ "items").asOpt[Seq[JsValue]].getOrElse(Nil)
            
        val ids = vidItems.map(a => ((a \ "id").asOpt[String].getOrElse(""), (a \ "snippet" \ "channelId").asOpt[String].getOrElse(""), (a \ "snippet" \ "title").asOpt[String].getOrElse("")))
        val chanIds = java.net.URLEncoder.encode(ids.foldLeft("")((a,b) => b._2 + "," + a).dropRight(1), "UTF-8")
        //Short-circuit if parsing or getting the channel list fails
        chanList(chanIds) match {
          case None => None
          case Some(chanList) => {
            val topicList = vidItems.map(a => {
                                           (a \ "topicDetails" \ "topicIds").asOpt[Seq[JsValue]].getOrElse(Nil) match {
                                             case Nil => "No Topic"
                                             case h::_ => h.asOpt[String].getOrElse("Fail")
                                            }
            }).filter(_!="No Topic").filter(_!="Fail").toSet                     

            val games = topicList.map(a => (a, (safeParse(BroadcastDataProcessor.get("https://kgsearch.googleapis.com/v1/entities:search?ids=" + 
                           a + "&key=" + conf.getString("YOUTUBE_API_KEY"), Array()).getOrElse("")).getOrElse(JsNull) \ "itemListElement")
                           .asOpt[Seq[JsValue]].getOrElse(Nil) match {
              case Nil =>  JsString("")
              case h::_ => (h \ "result" \ "name").asOpt[JsString].getOrElse(JsString(""))}))
              
                         //A bit of redundant error checking here, but it works                     
            val vidWithGameName = vidItems.map(a=> JsObject(List(("id", a \ "id"), ("viewers", a \ "liveStreamingDetails" \ "concurrentViewers"), ("game", 
                          getGameName((a \ "topicDetails" \ "topicIds").asOpt[Seq[JsValue]].getOrElse(Nil) match {
              case Nil => "No Topic"
              case h::_ => h.asOpt[String].getOrElse("Fail")}, games.toList)))))
                        
            val chanItems = (chanList \ "items").asOpt[Seq[JsValue]].getOrElse(Nil)
            
            //Return a reformed JsObject with required information.
            Some(ids.map(a => JsObject(List(("title", JsString(a._3)),("video",  getObj(a._1, vidWithGameName)), ("channel", getObj(a._2, chanItems))))))
          }
        }
      }
    }
}



        


      
    


   


