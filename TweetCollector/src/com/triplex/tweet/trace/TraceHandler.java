package com.triplex.tweet.trace;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TraceHandler
{
  private static TraceHandler traceHandler = null;
  /* Get actual class name to be printed on */
  private static Logger log = null;
  
  private TraceHandler(String className)
  {
    log = Logger.getLogger(className);
  }
  
  /**
   * @return {@link TraceHandler} the only TraceHandler instance
   */
  public static TraceHandler getOnlyInstance(String className)
  {
    PropertyConfigurator.configure("configProperties/log4j.properties");
    if(traceHandler == null)
      traceHandler = new TraceHandler(className);
    return traceHandler;
  }
  /**
   * @return
   */
  public Logger getLogger()
  {
    return log;
  }
}