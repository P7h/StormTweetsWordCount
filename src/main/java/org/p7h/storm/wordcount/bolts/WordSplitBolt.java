package org.p7h.storm.wordcount.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/**
 * Splits the tweets into words using space as the delimiter.
 *
 * @author - Prashanth Babu
 */
public final class WordSplitBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordSplitBolt.class);
	private static final long serialVersionUID = 4409872016715413315L;

	private final int minWordLength;
	private OutputCollector _collector;

	public WordSplitBolt(final int minWordLength) {
		this.minWordLength = minWordLength;
	}

	@Override
	public final void prepare(final Map map, final TopologyContext topologyContext,
	                          final OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public final void execute(final Tuple input) {
		final Status status = (Status) input.getValueByField("tweet");
		final String tweet = status.getText().replaceAll("\\p{Punct}", " ").toLowerCase();
		final String[] words = tweet.replaceAll("\n", " ").split(" ");
		final List<String> list = new ArrayList<>();
		for (final String word : words) {
			if (this.minWordLength < word.length()) {
				list.add(word);
			}
		}
		//Emit all words of a tweet in one go.
		this._collector.emit(new Values(list));
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
	}
}