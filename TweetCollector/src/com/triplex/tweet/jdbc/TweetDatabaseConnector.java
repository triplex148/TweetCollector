package com.triplex.tweet.jdbc;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Level;

import com.triplex.tweet.dol.std.Event;
import com.triplex.tweet.dol.std.TweetState;
import com.triplex.tweet.trace.TraceHandler;

/**
 * JBDC Database connector for tweet and event specific operations
 * 
 * @author Manny
 */
public class TweetDatabaseConnector
{
  private static TraceHandler traceHandler = TraceHandler.getOnlyInstance(TweetDatabaseConnector.class.getName());
  private static Connection connection;
  private static TweetDatabaseConnector singleInstance;

  /**
   * @return {@link TweetDatabaseConnector} the one and only instance
   */
  public static TweetDatabaseConnector getOnlyInstance()
  {
    if (singleInstance == null)
    {
      singleInstance = new TweetDatabaseConnector();
    }
    return singleInstance;
  }

  /**
   * C'tor for initializing the database connection
   */
  private TweetDatabaseConnector()
  {
    try
    {
      Properties p = new Properties();
      p.load(new FileInputStream("configProperties/ApplicationConfiguration.properties"));
      String hostname = (String) p.get("mysqlHost");
      String port = (String) p.get("mysqlPort");
      String userName = (String) p.get("mysqlUser");
      String pwd = (String) p.get("mysqlPwd");
      String dbName = (String) p.get("mysqlDbName");
      initializeDatabaseConnection("jdbc:mysql://" + hostname + ":" + port + "/" + dbName, userName, pwd);
    } catch (Exception ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
  }

  /**
   * Initializing the database connection
   * 
   * @param conUrl {@link String} the connection url
   * @param dbUser {@link String} the username
   * @param dbPwd {@link String} the db password
   * @return
   */
  public Connection initializeDatabaseConnection(String conUrl, String dbUser, String dbPwd)
  {
    if (connection == null)
    {
      try
      {
	// loads com.mysql.jdbc.Driver into memory
	Class.forName("com.mysql.jdbc.Driver");
	connection = DriverManager.getConnection(conUrl, dbUser, dbPwd);
	traceHandler.getLogger().log(Level.INFO, "DB-Connection successfully established");
      } catch (ClassNotFoundException cnf)
      {
	cnf.printStackTrace();
	traceHandler.getLogger().log(Level.ERROR, "Driver could not be loaded");
      } catch (SQLException e)
      {
	e.printStackTrace();
	traceHandler.getLogger().log(Level.ERROR, e.getMessage());
      }
    }
    return connection;
  }

  /**
   * Retrieving events from table <EVENT>. A parameter eventId could be 
   * specified in case a specific event is needed.
   * 
   * @param eventId {@link Integer} event object identifier
   * @return {@link List} list of event objects
   */
  public List<Event> readEventObjects(Integer eventId)
  {
    List<Event> ret = new ArrayList<Event>();
    try
    {
      StringBuilder prepQuery = new StringBuilder();
      prepQuery.append("SELECT id, event_title, event_description, event_from, event_to, "
          + "event_tw_count, event_state, event_tweet_tags FROM EVENT");
      
      if(eventId != null)
	prepQuery.append(" WHERE id = ?");
      
      PreparedStatement preparedStatement = connection.prepareStatement(prepQuery.toString());
      
      if(eventId != null)
	preparedStatement.setInt(1, eventId);
      
      ResultSet rs = preparedStatement.executeQuery();
      while (rs.next())
      {
	Integer id = rs.getInt("id");
	String title = rs.getString("event_title");
	String desc = rs.getString("event_description");
	Date dateFrom = rs.getDate("event_from");
	Date dateTo = rs.getDate("event_to");
	Integer twCount = rs.getInt("event_tw_count");
	String state = rs.getString("event_state");
	String tags = rs.getString("event_tweet_tags");
	Event ev = new Event(id, title, desc, dateFrom, dateTo, twCount, tags, state);
	ret.add(ev);
      }
      traceHandler.getLogger().log(Level.INFO, ret.size() + " Events are read from database");
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }
  
  /**
   * Inserting of an event object. Respectively for junit test cases
   *  
   * @param id {@link Integer} objectId
   * @param title {@link String} the event title
   * @param description {@link String} the event description
   * @param tags {@link String} the tweet tags for the event
   * @return boolean flag indicate the state of insert operation
   */
  public boolean insertEventObject(Integer id, String title, String description, String tags)
  {
    boolean ret = false;
    try
    {
      PreparedStatement preparedStatement = connection.prepareStatement("insert into event (id, event_title, event_description, event_tweet_tags) values (?, ?, ?, ?)");
      preparedStatement.setInt(1, id);
      preparedStatement.setString(2, title);
      preparedStatement.setString(3, description);
      preparedStatement.setString(4, tags);
      int i = preparedStatement.executeUpdate();
      
      if(i<=0)
	ret = false;
      else
	ret = true;
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }
  
  /**
   * Delete an event object from database with the given object id. Respectively needed for junit testcases
   * 
   * @param id {@link Integer} the event id to delete
   * @return boolean flag indicate the delete operation
   */
  public boolean deleteEventObject(Integer id)
  {
    boolean ret = false;
    try
    {
      PreparedStatement preparedStatement = connection.prepareStatement("delete from event where id = ?");
      preparedStatement.setInt(1, id);
      int i = preparedStatement.executeUpdate();
      
      if(i<=0)
	ret = false;
      else
	ret = true;
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }
  
  /**
   * Setting the event attribute event_state which indicate the current collection state of 
   * the specified event. 
   * 	- 0 indicate the starting state.
   * 	- 1 indicate the state collection started
   * 	- 2 indicate the state collection finished
   * 
   * @param eventId {@link Integer} the event object id
   * @param collectingState {@link String} the collecting state
   */
  public boolean updateEventCollectionState(Integer eventId, String collectingState)
  {
    boolean ret = false;
    try
    {
      PreparedStatement preparedStatement = connection.prepareStatement("update event set event_state = ? where id = ? ");
      preparedStatement.setString(1, collectingState);
      preparedStatement.setInt(2, eventId);

      int i = preparedStatement.executeUpdate();
      if (i <= 0)
      {
	traceHandler.getLogger().log(Level.INFO, "No update for event (" + eventId + ") executed");
      } else
      {
	ret = true;
	traceHandler.getLogger().log(Level.INFO, "Event (" + eventId + ") successfully updated: " + collectingState);
      }
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }

  /**
   * Inserting of an tweet entry into the database with all specified attributes. If an tweet entry still
   * exists in the database, the insert into will be ignored and no exception will be thrown.
   * 
   * @param state {@link TweetState} the current tweet entry to insert
   * @return int value of the insert process
   */
  public int insertTweetEntry(TweetState state)
  {
    int i = 0;
    try
    {
      PreparedStatement preparedStatement = connection.prepareStatement("insert ignore into tweet_entry (id, tw_text, tw_creationdate, tw_user, tw_location, tw_language, event_id) values (?, ?, ?, ?, ?, ?, ?)");
      preparedStatement.setLong(1, state.getTweetId());
      preparedStatement.setString(2, state.getTweetText());
      preparedStatement.setDate(3, new java.sql.Date(state.getTweetCreationDate().getTime()));
      preparedStatement.setString(4, state.getTweetUser());
      preparedStatement.setString(5, state.getTweetLocation());
      preparedStatement.setString(6, state.getTweetLanguage());
      preparedStatement.setInt(7, state.getEventId());
      i = preparedStatement.executeUpdate();
      if (i <= 0)
      {
	traceHandler.getLogger().log(Level.INFO, "Nothing inserted into database for Tweet: " + state);
	i=0;
      } else
      {
	traceHandler.getLogger().log(Level.INFO, "Tweet successfully inserted to db: " + state);
      }
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return i;
  }

  /**
   * Read all sentiment words from database and save them into a map with key the name and value the weight
   * 
   * @return {@link Map} of all sentiments with specific weight
   */
  public Map<String, Integer> readAllSentimentWords()
  {
    Map<String, Integer> ret = new HashMap<String, Integer>();
    try
    {
      StringBuilder prepQuery = new StringBuilder();
      prepQuery.append("SELECT id, sent_word, sent_weight, sent_language FROM sentiment_definition");
      
      PreparedStatement preparedStatement = connection.prepareStatement(prepQuery.toString());
      
      ResultSet rs = preparedStatement.executeQuery();
      while (rs.next())
      {
	String txt = rs.getString("sent_word");
	Integer weight = rs.getInt("sent_weight");
	//String lang = rs.getString("sent_language");
	ret.put(txt, weight);
      }
      traceHandler.getLogger().log(Level.INFO, ret.size() + " Sentiment words read from database");
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }
  
  /**
   * Read tweet entries from database. If a parameter eventId is specified, the tweets are 
   * read for the given event. If the parameter is not set, all tweet entries in database for
   * all events will be returned
   * 
   * @param eventIdParam {@link Integer} the event object id
   * @return {@link List} list of tweet entries read from database
   */
  public List<TweetState> readTweetEntries(Integer eventIdParam)
  {
    List<TweetState> ret = new ArrayList<TweetState>();
    try
    {
      StringBuilder prepQuery = new StringBuilder();
      prepQuery.append("SELECT id, tw_text, tw_creationdate, tw_user, tw_location, tw_language, tw_deleted, event_id, tw_weight  FROM tweet_entry");
      
      if(eventIdParam != null)
	prepQuery.append(" where event_id = ").append(eventIdParam);
      
      PreparedStatement preparedStatement = connection.prepareStatement(prepQuery.toString());
      
      ResultSet rs = preparedStatement.executeQuery();
      while (rs.next())
      {
	Long id = rs.getLong("id");
	String txt = rs.getString("tw_text");
	Date creationDate = rs.getDate("tw_creationdate");
	String user = rs.getString("tw_user");
	String loc = rs.getString("tw_location");
	String language = rs.getString("tw_language");
	Integer eventId = rs.getInt("event_id");
	Integer weight = rs.getInt("tw_weight");
	
	TweetState twState = new TweetState(id, txt, user, creationDate, loc, language, eventId);
	twState.setWeight(weight);
	ret.add(twState);
      }
      traceHandler.getLogger().log(Level.INFO, ret.size() + " tweet entries read from database");
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }
  
  /**
   * Single entrypoint for execute a update query for setting the tweet weight for a single tweet object
   * 
   * @param tweetId {@link Long} tweet identifier
   * @param weight {@link Integer} weight indicator
   * @return int for update state
   */
  public int updateEventTweetWeight(Long tweetId, Integer weight)
  {
    int i = 0;
    try
    {
      PreparedStatement preparedStatement = connection.prepareStatement("update tweet_entry set tw_weight = ? where id = ? ");
      preparedStatement.setInt(1, weight);
      preparedStatement.setLong(2, tweetId);

      i = preparedStatement.executeUpdate();
      if (i <= 0)
      {
	i=0;
      }
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return i;
  }
  
  /**
   * Delete an tweet entry object from database with the given object id. Respectively needed for junit testcases
   * 
   * @param id {@link Integer} the tweet entry id to delete
   * @return boolean flag indicate the delete operation
   */
  public boolean deleteTweetEntryObject(Long id)
  {
    boolean ret = false;
    try
    {
      PreparedStatement preparedStatement = connection.prepareStatement("delete from tweet_entry where id = ?");
      preparedStatement.setLong(1, id);
      int i = preparedStatement.executeUpdate();
      
      if(i<=0)
	ret = false;
      else
	ret = true;
    } catch (SQLException ex)
    {
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }
}