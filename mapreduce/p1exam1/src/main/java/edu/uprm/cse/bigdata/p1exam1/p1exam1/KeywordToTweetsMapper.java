package edu.uprm.cse.bigdata.p1exam1.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	String[] P1_WORDS = new String[] {"flu", "zika", "diarrhea", "ebola", "swamp", "change"};
        Set<String> P1_SET = new HashSet<String>(Arrays.asList(P1_WORDS));
    	
        String rawTweet = value.toString();
        String flu = "";
        String zika = "";
        String diarrhea = "";
        String ebola = "";
        String swamp = "";
        String change = "";

        try {
            Status tweetStatus = TwitterObjectFactory.createStatus(rawTweet);
            // Remove URL's, emojis and punctuation marks .,;:"'<>?/~`!@#$%^&*()_-=+[]{}! (leave only alphanumeric characters)
            String parsed_text = tweetStatus.getText().replaceAll("http.*?(\\s|$)|\\P{Print}|[^A-Za-z]+", " ").trim();
            String[] words = parsed_text.toLowerCase().split("\\s+");
            Long tweet_id = tweetStatus.getId();
            for (String word: words) {
                if(word.contains("flu"))
                	flu = flu.concat(tweet_id.toString()).concat(",");
                else if(word.contains("zika"))
                	zika = zika.concat(tweet_id.toString()).concat(",");
                else if(word.contains("diarrhea"))
                	diarrhea = diarrhea.concat(tweet_id.toString()).concat(",");
                else if(word.contains("ebola"))
                	ebola = ebola.concat(tweet_id.toString()).concat(",");
                else if(word.contains("swamp"))
                	swamp = swamp.concat(tweet_id.toString()).concat(",");
                else if(word.contains("change"))
                	change = change.concat(tweet_id.toString()).concat(",");
            }
            context.write(new Text("flu, "), new Text(flu));
            context.write(new Text("zika, "), new Text(zika));
            context.write(new Text("diarrhea, "), new Text(diarrhea));
            context.write(new Text("ebola, "), new Text(ebola));
            context.write(new Text("swamp, "), new Text(swamp));
            context.write(new Text("change, "), new Text(change));
        }
        catch(TwitterException e){
            // Ignore bad tweets
            Logger logger = LogManager.getRootLogger();
            logger.trace("Bad Tweet: " + rawTweet);
        }

    }

}