package org.p7h.storm.wordcount.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.util.List;
import java.util.Map;

/**
 * Splits the tweets into words using space as the delimiter.
 *
 * @author - Prashanth Babu
 */
public final class WordSplitBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordSplitBolt.class);
	private static final long serialVersionUID = -5789520783773808742L;

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
		//Replacing all punctuation marks and new lines in the tweet with an empty space.
		final String tweet = status.getText().replaceAll("\\p{Punct}|\\n", " ").toLowerCase();
		//Splitting the tweet on empty space.
		final Iterable<String> words = Splitter.on(' ')
                                             .trimResults()
                                             .omitEmptyStrings()
                                             .split(tweet);
		//Transform the words list by limiting to the words of a specific minimum word length.
		final List<String> list = Lists.newArrayList(
									Iterables.filter(
	                                   Iterables.transform(words, getOnlyWordsOfMinThresholdLength(minWordLength)),
	                                   Predicates.notNull()));
		//Emit all words of a tweet in one go.
		this._collector.emit(new Values(list));
	}

	/**
	 * Function to filter out the words which are not of minimum threshold as specified by the Topology and also Constructor of this class.
	 * @param minWordLength
	 * @return
	 */
	private static final Function<String, String> getOnlyWordsOfMinThresholdLength(final int minWordLength) {
		return new Function<String, String>() {
			@Override
			public final String apply(final String word) {
				String returnVal = null;
				if (minWordLength < word.length()) {
					returnVal = word;
				}
				return returnVal;
			}
		};
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
	}
}