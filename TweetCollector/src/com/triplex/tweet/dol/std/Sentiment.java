package com.triplex.tweet.dol.std;

public class Sentiment
{
  private int id;
  private String word;
  private int weight;
  public Sentiment()
  {
    super();
  }
  public Sentiment(int _id, String _word, int _weight)
  {
    this.id = _id;
    this.word = _word;
    this.weight = _weight;
  }
  public int getId()
  {
    return id;
  }
  public void setId(int id)
  {
    this.id = id;
  }
  public String getWord()
  {
    return word;
  }
  public void setWord(String word)
  {
    this.word = word;
  }
  public int getWeight()
  {
    return weight;
  }
  public void setWeight(int weight)
  {
    this.weight = weight;
  }
  
}