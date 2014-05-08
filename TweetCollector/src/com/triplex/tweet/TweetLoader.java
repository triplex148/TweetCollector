package com.triplex.tweet;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Level;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.triplex.tweet.dol.std.Event;
import com.triplex.tweet.dol.std.TweetState;
import com.triplex.tweet.jdbc.TweetDatabaseConnector;
import com.triplex.tweet.trace.TraceHandler;

/**
 * Tweet loader thread for retrieving tweets by a defined hashtag and loading
 * them into the database for an event id.
 * 
 * For this program, there are two seperated entrypoints:
 * 1) A program argument can be overgiven (only one argument is allowed ans is proofed) 
 * 	which indicate an event identifier. If an event id is overgiven, the program 
 * 	search after the event in the database, and if the event exists, a tweet scan will 
 * 	be processed
 * 2) If no argument is defined, all events in the system will be iterated and a tweet
 * 	scan will be executed.
 * 
 * @author Manny
 */
public class TweetLoader implements RateLimitStatusListener
{
  private static final TraceHandler traceHandler = TraceHandler.getOnlyInstance(TweetLoader.class.getName());
  private Twitter twitterStream; // Connection to Twitter public streaming endpoint

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    TweetLoader mainLoader = new TweetLoader();
    Properties p = new Properties();
    try{
      p.load(new FileInputStream("configProperties/ApplicationConfiguration.properties"));
    }catch(Exception ex)
    {
      traceHandler.getLogger().log(Level.ERROR, "Error on loading properties into system");
    }
    
    Integer eventId = null;
    if(args != null && args.length == 1)
    {
      //eventId on position 1
      eventId = Integer.valueOf(args[0]);
    }else if(args != null && args.length > 1)
    {
      traceHandler.getLogger().log(Level.ERROR, "Wrong count of program arguments. Only an eventId will be accepted!");
      System.exit(1);
    }
    
    // initialize the twitter api
    TweetDatabaseConnector connector = TweetDatabaseConnector.getOnlyInstance();
    mainLoader.initTwitterStream((String) p.get("consumerKey"),
				 (String) p.get("consumerSecret"),
				 (String) p.get("accessToken"),
				 (String) p.get("accessTokenSecret"));
    
    List<Event> allEvents = connector.getEventsForProcess(eventId);
    
    if(allEvents.isEmpty())
      traceHandler.getLogger().log(Level.INFO, "No events in the system to process new tweets");
    
    for(Event event : allEvents)
    {
      Date dateFrom = event.getDateFrom();
      Date dateTo = event.getDateTo();
      Date currentDate = Calendar.getInstance().getTime();
      
      if(between(currentDate, dateFrom, dateTo))
      {
	try
	{
	  // initialize the twitterstream with customer keys
	  connector.updateEventCollectionState(event.getEventId(), "1"); // update event to collecting active
	  Query query = new Query(event.getTags());
	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	  if(event.getDateFrom() != null)
	    query.setSince(sdf.format(event.getDateFrom()));
	  if(event.getDateTo() != null)
	    query.setUntil(sdf.format(event.getDateTo()));
	  List<Status> statuses = mainLoader.loadTweets(query);
	  int countTweetsInserted = 0;
	  for (int i = 0; i < statuses.size(); i++)
	  {
	    Status tweet = statuses.get(i);
	    if (tweet != null)
	    {
	      TweetState tw = new TweetState(tweet.getId(), tweet.getText(), tweet.getUser().getName(), new Date(tweet.getCreatedAt().getTime()), tweet.getUser().getLocation(), tweet.getUser().getLang(), event.getEventId());
	      countTweetsInserted+=connector.insertTweetEntry(tw);
	    }
	  }
	  connector.updateEventCollectionState(event.getEventId(), "2"); // update event to collecting finished
	  System.out.println("Event: " + event.getEventTitle() + " --> " + countTweetsInserted + " Tweets inserted into the system.");

	} catch (TwitterException ex)
	{
	  System.out.println(ex.getMessage());
	  traceHandler.getLogger().log(Level.ERROR, "Twitter Failure: " + ex.getMessage());
	}
      }
    }
    
    /**
     * Start the sentiment evaluation
     */
    Map<String, Integer> sentiments = connector.initializeSentimentWords();
    List<TweetState> allTweets = connector.getAllTweetEntries();
    // Sentiment with 
    int j=0;
    for(int i=0;i<allTweets.size();i++)
    {
      TweetState cur = allTweets.get(i);
      String tweetTxt = cur.getTweetText();
      StringTokenizer st = new StringTokenizer(tweetTxt);
      int summary = 0;
      int analysisCount = 0;
      int wordCount = 0;
      while(st.hasMoreTokens())
      {
	String s = st.nextToken().toLowerCase();
	
	if(sentiments.containsKey(s))
	{
	  Integer weight = sentiments.get(s);
	  analysisCount++;
	  summary +=weight;
	}
	wordCount++;
      }
      // now calculate the weight and save it to the tweet
      /*
       * this was a good movie but i hate the end of the day
       * -4
       */
      if(cur.getWeight() != summary)
      {
	cur.setWeight(summary);
	connector.updateEventTweetWeight(cur.getTweetId(), cur.getWeight());
	j++;
      }
    }
    System.out.println(j + " Tweets are evaluated");
  }

  /**
   * Checks, if a date is between two other dates
   * @param date
   * @param dateStart
   * @param dateEnd
   * @return
   */
  public static boolean between(Date date, Date dateStart, Date dateEnd)
  {
    if (date != null && dateStart != null && dateEnd != null)
    {
      if (date.after(dateStart) && date.before(dateEnd))
      {
	return true;
      } else
      {
	return false;
      }
    }
    return false;
  }

  /**
   * Initialize the twitter stream (api) with needed keys and tokens
   * 
   * @param consumerKey
   * @param consumerSecret
   * @param accessToken
   * @param accessTokenSecret
   */
  public void initTwitterStream(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret)
  {
    if (getTwitterStream() == null)
    {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.setDebugEnabled(true);
      cb.setOAuthConsumerKey(consumerKey);
      cb.setOAuthConsumerSecret(consumerSecret);
      cb.setOAuthAccessToken(accessToken);
      cb.setOAuthAccessTokenSecret(accessTokenSecret);
      cb.setJSONStoreEnabled(true);

      Twitter twitter = new TwitterFactory(cb.build()).getInstance();
      twitter.addRateLimitStatusListener(this);
      setTwitterStream(twitter);
    }
  }

  /**
   * Load tweets by a given query stream
   * 
   * @param query
   * @return {@link List} of Tweet entries
   * @throws TwitterException
   */
  public List<Status> loadTweets(Query query) throws TwitterException
  {
    List<Status> returnValue = new ArrayList<Status>();
    if (getTwitterStream() != null && query != null)
    {
      int oldSize = 0;
      Set<Long> set = new HashSet<Long>();

      QueryResult result = getTwitterStream().search(query);
      
      do
      {
	List<Status> tweets = result.getTweets();

	for (Status tweet : tweets)
	{
	  oldSize = set.size();
	  set.add(tweet.getId());
	  if (set.size() > oldSize)
	  {
	    oldSize = set.size();
	    returnValue.add(tweet);
	  }
	}
	// get tweets in the next page if any
	query = result.nextQuery();
	if (query != null)
	  result = getTwitterStream().search(query);

      } while (query != null);
    }
    return returnValue;
  }

  /**
   * Saved instance of twitter stream api
   * 
   * @return {@link Twitter} factory build
   */
  public Twitter getTwitterStream()
  {
    return twitterStream;
  }

  /**
   * Set the twitter stream api factory
   * 
   * @param twitterStream
   *          {@link Twitter} stream api
   */
  public void setTwitterStream(Twitter twitterStream)
  {
    this.twitterStream = twitterStream;
  }

  /**
   * @see twitter4j.RateLimitStatusListener#onRateLimitReached(twitter4j.RateLimitStatusEvent)
   */
  @Override
  public void onRateLimitReached(RateLimitStatusEvent arg0)
  {
    traceHandler.getLogger().log(Level.ERROR, "Twitter Failure: " + arg0.getRateLimitStatus());
    System.exit(0);
  }

  /**
   * @see twitter4j.RateLimitStatusListener#onRateLimitStatus(twitter4j.RateLimitStatusEvent)
   */
  @Override
  public void onRateLimitStatus(RateLimitStatusEvent arg0)
  {
  }
}