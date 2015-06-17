/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.illecker.sentistorm.spout;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.international.arabic.Buckwalter;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import at.illecker.sentistorm.commons.SentimentClass;
import at.illecker.sentistorm.commons.util.TimeUtils;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterFileSpout extends BaseRichSpout {
  public static final String ID = "twitter-file-spout";
  public static final String CONF_STARTUP_SLEEP_MS = ID + ".startup.sleep.ms";
  private static final long serialVersionUID = -4658730220755697034L;
  private static final int UPDATE_TIME_THRESHOLD = 10000; //10 sec
  private SpoutOutputCollector m_collector;
  private LinkedBlockingQueue<Status> m_tweetsQueue = null;
  private String m_filterLanguage;
  private long input_rate;
  private long input_timestamp;
  private static final Logger LOG = LoggerFactory
	      .getLogger(TwitterFileSpout.class);
  private static String path;
  
  public TwitterFileSpout(String path){
	  this.path=path;
	  if(path==null){
		  path="tweets/tweets.txt";
	  }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // key of output tuples
//    declarer.declare(new Fields("id", "text", "score","rate"));
    declarer.declare(new Fields("id", "text", "score"));
  }

  @Override
  public void open(Map config, TopologyContext context,
      SpoutOutputCollector collector) {
    m_collector = collector;
    m_tweetsQueue = new LinkedBlockingQueue<Status>(2048);
    input_rate=0;
    input_timestamp=System.currentTimeMillis();
    // Optional startup sleep to finish bolt preparation
    // before spout starts emitting
    if (config.get(CONF_STARTUP_SLEEP_MS) != null) {
      long startupSleepMillis = (Long) config.get(CONF_STARTUP_SLEEP_MS);
      TimeUtils.sleepMillis(startupSleepMillis);
    }

    BufferedReader reader;
    LOG.info("reading file: "+path);
    try {
//		ClassLoader classLoader = getClass().getClassLoader();
//		File file =new File(classLoader.getResource(path).getFile());
//	    reader=new BufferedReader(new FileReader(path));

	    LOG.info("starting reading");
	} catch (Exception ex) {
		LOG.error("Error while reading file", ex);
		LOG.trace("", ex);
	}

  }

  @Override
  public void nextTuple() {
    Status tweet = m_tweetsQueue.poll();
    if (tweet == null) {
    	// send a random tweet if the queue is empty
    	m_collector.emit(new Values("123456", "Why this is happening to me !!!", null));
      TimeUtils.sleepMillis(2); // sleep 1 ms
    } else {
      // Emit tweet
//      m_collector.emit(new Values(tweet.getId(), tweet.getText(), null, input_rate));
      m_collector.emit(new Values(tweet.getId(), tweet.getText(), null));
      input_rate++;
      if(System.currentTimeMillis() - input_timestamp > UPDATE_TIME_THRESHOLD){
      	LOG.info("rate:"+input_rate);
      	input_rate=0;
      	input_timestamp=System.currentTimeMillis();
      }
    }
  }


  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config ret = new Config();
    ret.setMaxTaskParallelism(1);
    return ret;
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }
}
