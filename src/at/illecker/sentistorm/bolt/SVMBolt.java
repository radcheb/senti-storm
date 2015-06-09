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
package at.illecker.sentistorm.bolt;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.illecker.sentistorm.commons.Configuration;
import at.illecker.sentistorm.commons.Dataset;
import at.illecker.sentistorm.commons.SentimentClass;
import at.illecker.sentistorm.commons.svm.SVM;
import at.illecker.sentistorm.commons.util.io.SerializationUtils;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SVMBolt extends BaseBasicBolt {
	public static final String ID = "support-vector-maschine-bolt";
	public static final String CONF_LOGGING = ID + ".logging";
	private static final int UPDATE_THRESHOLD = 100;
	private static final long serialVersionUID = -6790858930924043126L;
	private static final Logger LOG = LoggerFactory.getLogger(SVMBolt.class);
	private boolean m_logging = true;
	private Dataset m_dataset;
	private svm_model m_model;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// no output tuples
		declarer.declare(new Fields("text", "score"));
//		declarer.declare(new Fields("text", "score","rate"));
	}

	@Override
	public void prepare(Map config, TopologyContext context) {
		// Optional set logging
		if (config.get(CONF_LOGGING) != null) {
			m_logging = (Boolean) config.get(CONF_LOGGING);
		} else {
			m_logging = false;
		}
		LOG.info("Loading SVM model...");
		m_dataset = Configuration.getDataSetSemEval2013();
		m_model = SerializationUtils.deserialize(m_dataset.getDatasetPath()
				+ File.separator + SVM.SVM_MODEL_FILE_SER);

		if (m_model == null) {
			LOG.error("Could not load SVM model! File: "
					+ m_dataset.getDatasetPath() + File.separator
					+ SVM.SVM_MODEL_FILE_SER);
			throw new RuntimeException();
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String text = tuple.getStringByField("text");
//	    long input_rate=tuple.getLongByField("rate");
		Map<Integer, Double> featureVector = (Map<Integer, Double>) tuple
				.getValueByField("featureVector");

		// Create feature nodes
		svm_node[] testNodes = new svm_node[featureVector.size()];
		int i = 0;
		for (Map.Entry<Integer, Double> feature : featureVector.entrySet()) {
			svm_node node = new svm_node();
			node.index = feature.getKey();
			node.value = feature.getValue();
			testNodes[i] = node;
			i++;
		}

		double predictedClass = svm.svm_predict(m_model, testNodes);

		SentimentClass score = SentimentClass.fromScore(m_dataset,
				(int) predictedClass);
		int numerical_score=0;
		if (score == SentimentClass.NEGATIVE) {
			numerical_score=-1;
		} else if (score == SentimentClass.POSITIVE) {
			numerical_score=1;
		}
		if (m_logging) {
			LOG.info("Tweet: " + text + " predictedSentiment: " + score);
		}
		collector.emit(new Values(text,numerical_score));
//		collector.emit(new Values(text,score,input_rate));
	}

}
