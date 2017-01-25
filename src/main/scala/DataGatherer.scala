import tv.tsagh.datacollection.PlatformFunctionDefinitions
import tv.tsagh.datacollection.BroadcastDataProcessor._
import tv.tsagh.datacollection._

object DataGatherer {
  
  def main(args:Array[String]): Unit = {
    //Define timestamp
    val ts = new java.sql.Timestamp(new java.util.Date().getTime())
    
    //Pulling results from Twitch
    val twitch = buildWhile(
        Nil,
        ts,
        10,  //Will retry fail 10 times
        "0", //Page offset is an integer value starting with zero
        PlatformFunctionDefinitions.twitchPFuncs)
        
    //Pulling results from Beam
    val beam = buildWhile(
        Nil, 
        ts, 
        10,  //Will retry fail 10 times
        "0", //Page offset is an integer value starting with zero
        PlatformFunctionDefinitions.beamPFuncs
        )
    
    //Pulling results from HitBox
    val hitbox = buildWhile(
        Nil, 
        ts, 
        10,  //Will retry fail 10 times
        "0", //Page offset is an integer value starting with zero
        PlatformFunctionDefinitions.hitboxPFuncs)
        
    //Pulling results from Abuzu
    val abuzu = buildWhile(
        Nil,
        ts,
        10,  //Will retry fail 10 times
        "0", //Page offset is an integer value starting with zero
        PlatformFunctionDefinitions.abuzuPFuncs)
        
    //Pulling results from YouTube
    val youTube = buildWhile(
        Nil,
        ts,
        10,  //Will retry fail 10 times
        "",  //Page offset is a string nextPageToken capture, received as part of each packet.
        PlatformFunctionDefinitions.youtubePFuncs
        )
        
    
    println("Number of active streams meeting criteria:")
    println("Twitch: " + removeDuplicates(twitch, Nil).size)
    println("HitBox: " + removeDuplicates(hitbox, Nil).size)
    println("Abuzu: " + removeDuplicates(abuzu, Nil).size)
    println("Beam: " + removeDuplicates(beam, Nil).size)
    println("YouTube: " + removeDuplicates(youTube, Nil).size)
  }
  
  //Tail-safe recursive function for removing duplicates. Could potentially be written as foldLeft
    @annotation.tailrec
    def removeDuplicates(l: List[Snapshot], n: List[Snapshot]): List[Snapshot] = {
      l match {
        case h::t => removeDuplicates(t, h::n.filter(_!=h.c))
        case Nil => n
      }
    }
}