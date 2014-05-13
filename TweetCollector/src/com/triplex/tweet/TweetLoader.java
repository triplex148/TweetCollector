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
 * Tweet loader for retrieving tweets by a defined hashtag and loading
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
    
    List<Event> allEvents = connector.readEventObjects(eventId);
    
    if(allEvents.isEmpty())
      traceHandler.getLogger().log(Level.INFO, "No events in the system to process new tweets");
    
    for(Event event : allEvents)
    {
      try{
        // start the event process (query tweets ..)
        mainLoader.processEvent(event, true, connector);
      }catch(TwitterException e)
      {
	break;
      }
    }
    
    mainLoader.processSentimentEvaluation(connector);
  }

  /**
   * Process the given event with following specifications:
   * 	- Check the from and to date of the given event. If the current date is between the two dates, the event will be processed
   * 	- Then the event collection state will be set to 1 (collection active/started)
   * 	- Tweets will be loaded from twitter4j api for the given tags/datefrom/dateto
   * 	- Tweets will be inserted into database if not exists
   * 	- Finally the event collection state will be set to 2 (collection not active/finished)
   * 
   * @param event {@link Event} the given event object for which the process starts
   * @param boolean flag indicate if the twitter4J api should be triggered. This is used for junit tests, because junit doesn't need
   * 			a twitter4J api call at least
   * @param connector {@link TweetDatabaseConnector} the connector
   * @return boolean flag if the event is processed (the current date is between from and to date of the event)
   */
  public boolean processEvent(Event event, boolean queryTweets, TweetDatabaseConnector connector) throws TwitterException
  {
    boolean ret = false;
    
    Date dateFrom = event.getDateFrom();
    Date dateTo = event.getDateTo();
    Date currentDate = Calendar.getInstance().getTime();
    
    if(between(currentDate, dateFrom, dateTo))
    {
      ret = true;
      try
      {
	// initialize the twitterstream with customer keys
	connector.updateEventCollectionState(event.getEventId(), Event.COLLECTION_STATE_ACTIVE); // update event to collecting active
	if(queryTweets)
	{
	  Query query = new Query(event.getTags());
	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	  if(event.getDateFrom() != null)
	    query.setSince(sdf.format(event.getDateFrom()));
	  if(event.getDateTo() != null)
	    query.setUntil(sdf.format(event.getDateTo()));
	    
	  List<Status> statuses = loadTweets(query);
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
	  System.out.println("Event: " + event.getEventTitle() + " --> " + countTweetsInserted + " Tweets inserted into the database.");
	  traceHandler.getLogger().log(Level.INFO, "Event: " + event.getEventTitle() + " --> " + countTweetsInserted + " Tweets inserted into the database.");
	}
	connector.updateEventCollectionState(event.getEventId(), Event.COLLECTION_STATE_FINISHED); // update event to collecting finished
      } catch (TwitterException ex)
      {
	ex.printStackTrace();
	traceHandler.getLogger().log(Level.ERROR, "Twitter Failure: " + ex.getMessage());
      }
    }
    return ret;
  }
  
  /**
   * Start the sentiment evaluation.
   * 
   * Steps to complete the sentiment analysis
   * 	- Read all sentiment words and all tweet entries into the memory
   * 	- Iteration over all tweet entries and processing one after another with the following handling:
   *		- Each word in an tweet entry text will be analysed and compared with the sentiment
   *		  words and calculated their sentiment weight.
   */
  public void processSentimentEvaluation(TweetDatabaseConnector connector)
  {
    Map<String, Integer> sentiments = connector.readAllSentimentWords();
    List<TweetState> allTweets = connector.readTweetEntries(null);
    int j=0;
    for(int i=0;i<allTweets.size();i++)
    {
      TweetState cur = allTweets.get(i);
      String tweetTxt = cur.getTweetText();
      StringTokenizer st = new StringTokenizer(tweetTxt);
      int summary = 0;
      int analysisCount = 0;
      while(st.hasMoreTokens())
      {
  	String s = st.nextToken().toLowerCase();
  	
  	if(sentiments.containsKey(s))
  	{
  	  Integer weight = sentiments.get(s);
  	  analysisCount++;
  	  summary +=weight;
  	}
      }
      /*
       * now calculate the weight and save it to the tweet
       * 
       * this was a good movie but i hate the end of the day
       * -10
       */
      if(analysisCount > 0)
      {
        summary = summary / analysisCount;
        if(cur.getWeight() != summary)
        {
          cur.setWeight(summary);
          connector.updateEventTweetWeight(cur.getTweetId(), cur.getWeight());
          j++;
        }
      }
    }
    System.out.println(j + " Tweets are new evaluated and analysed");
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
  }

  /**
   * @see twitter4j.RateLimitStatusListener#onRateLimitStatus(twitter4j.RateLimitStatusEvent)
   */
  @Override
  public void onRateLimitStatus(RateLimitStatusEvent arg0)
  {
  }
}