package com.triplex.tweet.jdbc;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;

import com.triplex.tweet.dol.std.Event;
import com.triplex.tweet.dol.std.TweetState;
import com.triplex.tweet.trace.TraceHandler;

/**
 * Database connector for tweet and event things
 * 
 * @author Manny
 */
public class TweetDatabaseConnector
{
  private static TraceHandler traceHandler = TraceHandler.getOnlyInstance(TweetDatabaseConnector.class.getName());
  private static Connection connection;
  private static TweetDatabaseConnector singleInstance;

  /**
   * @return
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
   * 
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
      System.out.println(ex.getMessage());
      ex.printStackTrace();
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
  }

  /**
   * @param conUrl
   * @param dbUser
   * @param dbPwd
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
	System.out.println(cnf.getMessage());
	traceHandler.getLogger().log(Level.ERROR, "Driver could not be loaded");
      } catch (SQLException e)
      {
	System.out.println(e.getMessage());
	traceHandler.getLogger().log(Level.ERROR, e.getMessage());
      }
    }
    return connection;
  }

  /**
   * @return
   */
  public List<Event> getEventsForProcess(Integer eventId)
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
	try
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
	} catch (SQLException ex)
	{
	  traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
	}
      }
      traceHandler.getLogger().log(Level.INFO, "Events read into system. Count: " + ret.size());
    } catch (SQLException ex)
    {
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
    return ret;
  }

  public void updateEventCollectionState(Integer eventId, String collectingState)
  {
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
	traceHandler.getLogger().log(Level.INFO, "Event (" + eventId + ") successfully updated: " + collectingState);
      }
    } catch (SQLException ex)
    {
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
  }

  /**
   * @param state
   */
  public void insertTweetEntry(TweetState state)
  {
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
      int i = preparedStatement.executeUpdate();
      if (i <= 0)
      {
	traceHandler.getLogger().log(Level.INFO, "Nothing inserted into database for Tweet: " + state);
      } else
      {
	traceHandler.getLogger().log(Level.INFO, "Tweet successfully inserted to db: " + state);
      }
    } catch (SQLException ex)
    {
      traceHandler.getLogger().log(Level.ERROR, ex.getMessage());
    }
  }
}