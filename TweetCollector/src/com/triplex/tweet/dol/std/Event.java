package com.triplex.tweet.dol.std;

import java.util.Date;

public class Event
{
  public static final String COLLECTION_STATE_ACTIVE = "1";
  public static final String COLLECTION_STATE_FINISHED = "2";
  private Integer eventId;
  private String eventTitle;
  private String eventDesc;
  private Date dateFrom;
  private Date dateTo;
  private Integer tweetCount;
  private String tags;
  private String state;
  public Event()
  {
    super();
  }
  public Event(Integer id, String title, String desc, Date from, Date to, Integer count, String tags, String state)
  {
    this.eventId = id;
    this.eventTitle = title;
    this.eventDesc = desc;
    this.dateFrom = from;
    this.dateTo = to;
    this.state = state;
    this.tags = tags;
    this.tweetCount = count;
  }
  public Integer getEventId()
  {
    return eventId;
  }
  public void setEventId(Integer eventId)
  {
    this.eventId = eventId;
  }
  public String getEventTitle()
  {
    return eventTitle;
  }
  public void setEventTitle(String eventTitle)
  {
    this.eventTitle = eventTitle;
  }
  public String getEventDesc()
  {
    return eventDesc;
  }
  public void setEventDesc(String eventDesc)
  {
    this.eventDesc = eventDesc;
  }
  public Date getDateFrom()
  {
    return dateFrom;
  }
  public void setDateFrom(Date dateFrom)
  {
    this.dateFrom = dateFrom;
  }
  public Date getDateTo()
  {
    return dateTo;
  }
  public void setDateTo(Date dateTo)
  {
    this.dateTo = dateTo;
  }
  public Integer getTweetCount()
  {
    return tweetCount;
  }
  public void setTweetCount(Integer tweetCount)
  {
    this.tweetCount = tweetCount;
  }
  public String getTags()
  {
    return tags;
  }
  public void setTags(String tags)
  {
    this.tags = tags;
  }
  public String getState()
  {
    return state;
  }
  public void setState(String state)
  {
    this.state = state;
  }
  @Override
  public String toString()
  {
    return eventTitle;
  }
}