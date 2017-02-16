/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterapi;

import java.util.Arrays;
import java.util.List;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author Charmal
 */
public class SearchQuery {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        try {

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey("")
                    .setOAuthConsumerSecret("")
                    .setOAuthAccessToken("")
                    .setOAuthAccessTokenSecret("");
            
            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();

            int recordCount = 200;

            long[] idArray = new long[recordCount];
            int index = 0;
            Query query = new Query("TheMasters"); // Wimbledon
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {

                    long twitterId = tweet.getId();

                    if (index < recordCount && !tweet.isRetweeted()) {

                        if (!Arrays.asList(idArray).contains(twitterId)) {
                            idArray[index] = twitterId;
                            index++;

                            System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText()); // Favourite 
                        }
                    } else {
                        System.exit(0);
                    }
                }
            } while ((query = result.nextQuery()) != null);
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }

}
