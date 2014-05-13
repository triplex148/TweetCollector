package com.triplex.junit;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * The test suite is responsible for defining all junit test classes.
 * So it is possible to start all junit tests per once and see the 
 * finally result.
 * 
 * @author Manny
 */
public class TweetTestSuite extends TestSuite
{
  public static void main(String[] args)
  {
    TestRunner.run(suite());
  }

  /**
   * @return
   */
  public static Test suite()
  {
    TestSuite suite = new TestSuite("Tweet tests");
    suite.addTestSuite(JUnitTweetDbTest.class);
    return suite;
  }
}