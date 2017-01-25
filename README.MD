BroadcastData is a system for collecting viewership data from livestreaming services.


Initial setup:

Adjust application.conf to reflect the values for Twitch or Youtube API keys.


Notable files: 

BroadcastDataProcessor.scala


This file contains platform generic functional code to process raw json strings from http requests into Snapshot objects
which store information including the channel, game, number of viewers, number of followers, and platform.


PlatformFunctionDefinitions.scala


This file contains example implementations of PlatformFunctions, which are used in BroadcastDataProcessor for Twitch, Abuzu,
Hitbox, Beam, and YouTube. The sample filters include follower/subscriber counts and broadcast language.


DataGatherer.scala


This file is a sample application that pulls data from all sources and outputs the number of relevant broadcasts active on each platform
Note: Because of how the PlatformFunction defines access to YouTube's API, only 200 broadcasts are filtered.