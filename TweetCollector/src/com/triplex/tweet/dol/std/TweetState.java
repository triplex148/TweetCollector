package com.triplex.tweet.dol.std;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TweetState
{
  private long tweetId;
  private String tweetText;
  private String tweetUser;
  private Date tweetCreationDate;
  private String tweetLocation;
  private String tweetLanguage;
  private int eventId;
  public TweetState()
  {
  }
  public TweetState(long twId, String txt, String user, Date date, String loc, String lang, int eventId)
  {
    this.tweetId = twId;
    this.tweetText = txt;
    this.tweetUser = user;
    this.tweetCreationDate = date;
    this.tweetLocation = loc;
    this.tweetLanguage = lang;
    this.eventId = eventId;
  }
  public String getTweetText()
  {
    return tweetText;
  }
  public void setTweetText(String tweetText)
  {
    this.tweetText = tweetText;
  }
  public String getTweetUser()
  {
    return tweetUser;
  }
  public void setTweetUser(String tweetUser)
  {
    this.tweetUser = tweetUser;
  }
  public Date getTweetCreationDate()
  {
    return tweetCreationDate;
  }
  public void setTweetCreationDate(Date tweetCreationDate)
  {
    this.tweetCreationDate = tweetCreationDate;
  }
  public String getTweetLocation()
  {
    return tweetLocation;
  }
  public void setTweetLocation(String tweetLocation)
  {
    this.tweetLocation = tweetLocation;
  }
  public String getTweetLanguage()
  {
    return tweetLanguage;
  }
  public void setTweetLanguage(String tweetLanguage)
  {
    this.tweetLanguage = tweetLanguage;
  }
  public int getEventId()
  {
    return eventId;
  }
  public void setEventId(int eventId)
  {
    this.eventId = eventId;
  }
  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    StringBuilder ret = new StringBuilder();
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
    ret.append("Id: ").append(tweetId).append("| User: ").append(tweetUser).append(" - ").append(sdf.format(tweetCreationDate));
    return ret.toString();
  }
  public long getTweetId()
  {
    return tweetId;
  }
  public void setTweetId(long tweetId)
  {
    this.tweetId = tweetId;
  }
}