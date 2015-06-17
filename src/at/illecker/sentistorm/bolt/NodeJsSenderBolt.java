package at.illecker.sentistorm.bolt;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.illecker.sentistorm.commons.SentimentClass;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class NodeJsSenderBolt extends BaseRichBolt {

	public static final String ID = "node-js-sender-bolt";
	public static final String CONF_LOGGING = ID + ".logging";
	public static final String CONF_NODESERVER = ID + ".nodeserver";
	private static final int UPDATE_TWEETS_THRESHOLD = 100;
	private static final int UPDATE_TIME_THRESHOLD = 1000;
	private static final long serialVersionUID = -6790858931924043126L;
	private static final Logger LOG = LoggerFactory
			.getLogger(NodeJsSenderBolt.class);
	private boolean m_logging = false;
	private int positves, negatives, neutres, total;
	private String webserver;
	private HttpClient client;
	private long timestamp = 0;

	transient CountMetric _countMetric;
	transient MultiCountMetric _wordCountMetric;
	transient ReducedMetric _wordLengthMeanMetric;

	@Override
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {

		if (config.get(CONF_LOGGING) != null) {
			m_logging = (Boolean) config.get(CONF_LOGGING);
			webserver = (String) config.get(CONF_NODESERVER);
		} else {
			m_logging = false;
		}

		if (config.get(CONF_NODESERVER) != null) {
			webserver = (String) config.get(CONF_NODESERVER);
		} else {
			webserver = "http://172.17.42.1:3001/post";
		}
		positves = 0;
		negatives = 0;
		total = 0;
		neutres = 0;
		reconnect();
		timestamp = System.currentTimeMillis();
		initMetrics(context);
	}
	void initMetrics(TopologyContext context)
	{
	    _countMetric = new CountMetric();
	    _wordCountMetric = new MultiCountMetric();
	    _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
	    
	    context.registerMetric("execute_count", _countMetric, 5);
	    context.registerMetric("word_count", _wordCountMetric, 60);
	    context.registerMetric("word_length", _wordLengthMeanMetric, 60);
	}
	private void reconnect() {
		this.client = HttpClientBuilder.create().build();
	}

	private void updateUI(long input_rate) {
		HttpPost post = new HttpPost(this.webserver);
		String content = String.format(

		"{\"total\": %d, " + "\"positive\": %d, " + "\"negative\": %d, "
				+ "\"neutre\": %d, " + "\"input_rate\": %d }", total, positves,
				negatives, neutres, (int)input_rate);
		try {
			post.setEntity(new StringEntity(content));
			HttpResponse response = client.execute(post);
			org.apache.http.util.EntityUtils.consume(response.getEntity());
			if (m_logging) {
				LOG.info("rate: " + total + " tweets");
			}
			positves = 0;
			total = 0;
			negatives = 0;
			neutres = 0;
		} catch (IOException e) {
			LOG.error("exception thrown while attempting post", e);
			LOG.trace(null, e);
			reconnect();
		} catch (NullPointerException e) {
			LOG.error("exception thrown while attempting post", e);
			LOG.debug("server: " + this.webserver);
			LOG.debug("client: " + client.toString());
			LOG.debug("server: " + post.toString());
			LOG.trace(null, e);
		}

	}

	@Override
	public void execute(Tuple tuple) {
		int score = tuple.getIntegerByField("score");
//		long input_rate = tuple.getLongByField("rate");
		long input_rate=0;
		total++;
		if (score == -1) {
			negatives++;
		} else if (score == 1) {
			positves++;
		} else {
			neutres++;
		}
		if (System.currentTimeMillis() - timestamp >= UPDATE_TIME_THRESHOLD) {
			timestamp = System.currentTimeMillis();
			updateUI(input_rate);
		}
     	 updateMetrics("ok");
	}
	void updateMetrics(String word)
	{
	  _countMetric.incr();
	  _wordCountMetric.scope(word).incr();
	  _wordLengthMeanMetric.update(word.length());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
