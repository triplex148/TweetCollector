package com.triplex.tweet;

/**
 * Class which is used to call the TweetLoader at given intervals, so that tweets can be periodically
 * fetched and the Twitteranalyser server.
 * @author mkesselb
 */
public class TweetTimeStarter
{
	public static void main(String[] args)
	{
		//call tweet loader and sleep for 30 minutes
		while(true)
		{
			TweetLoader.main(args);
			try 
			{
				Thread.sleep(1000L * 60L * 30L);
			} catch (InterruptedException e)
			{
				System.err.println("interrupted.");
				e.printStackTrace();
			}
		}
	}
}