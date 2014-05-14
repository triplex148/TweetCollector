package com.triplex.junit;

import junit.framework.TestCase;

import com.triplex.tweet.jdbc.TweetDatabaseConnector;

/**
 * Junit test case class for all tests belongig to database functionality.
 * For every testcase, a new method should be created with a well defined 
 * name. (test_nr_nameOfTestCase()). Each testcase should work with new created
 * objects (also on database) and finally after the testcase, the objects should
 * be deleted from database so no testdata remain on system..
 * 
 * @author Manny
 */
public class JUnitTweetDbTest extends TestCase
{
  /**
   * TODO comment the testcase
   */
  public void test_01_checkDatabaseConnection()
  {
    TweetDatabaseConnector connector = TweetDatabaseConnector.getOnlyInstance();
    assertNotNull(connector);
  }
  /**
   * TODO comment the testcase
   */
  public void test_02_NAME_TEST_CASE_CONVENTION()
  {
    TweetDatabaseConnector connector = TweetDatabaseConnector.getOnlyInstance();
    assertNotNull(connector);
    //first insert a temp event object wit dummy values
    //then read the event object and assert the result
    // finally in try catch finally block, delete the newly created event so no test data remains in the system
    assertNotNull(connector.readEventObjects(1));
    assertNull(connector.readEventObjects(1));
  }
}
