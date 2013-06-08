package org.p7h.storm.wordcount.bolts;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts the words and displays to the console and also logs to the file.
 *
 * @author - Prashanth Babu
 */
public final class WordCountBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountBolt.class);
	private static final long serialVersionUID = -4887737292363526370L;
	/** Interval between logging the output. */
    private final long logIntervalInSeconds;
	/** Log only the words which crosses this threshold value. */
	private final long minWordCountThreshold;

	private long lastLoggedTimestamp;
	private Map<String, MutableLong> wordCountTracker;
	private Multimap<Long, String> frequencyOfWords;
	private long runCounter;

    public WordCountBolt(final long logIntervalInSeconds, final long minWordCountThreshold) {
        this.logIntervalInSeconds = logIntervalInSeconds;
	    this.minWordCountThreshold = minWordCountThreshold;
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
                              final OutputCollector collector) {
        lastLoggedTimestamp = System.currentTimeMillis();
	    wordCountTracker = Maps.newHashMap();
	    //Doing this circus so that the output is in a proper ascending order of the calculated count of words.
	    frequencyOfWords = Multimaps.newListMultimap(
             new TreeMap<Long, Collection<String>>(), new Supplier<List<String>>() {
                 public List<String> get() {
                     return Lists.newArrayList();
                 }
             }
	    );
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void execute(final Tuple input) {
	    final List<String> words = (List<String>) input.getValueByField("words");
	    MutableLong count;
	    for (final String word : words) {
		    count = wordCountTracker.get(word);
		    if (null == count) {
			    wordCountTracker.put(word, new MutableLong());
		    } else {
			    count.increment();
		    }
	    }

	    final long timestampNow = System.currentTimeMillis();
	    final long logPeriodInSeconds = (timestampNow - lastLoggedTimestamp) / 1000;
        if (logPeriodInSeconds > logIntervalInSeconds) {
	        ++runCounter;
	        logWordCount();
            lastLoggedTimestamp = timestampNow;
        }
    }

    private final void logWordCount() {
	    long count;
	    String word;
	    //Group words based on the count into a Multimap
	    for (final Map.Entry<String, MutableLong> entry : wordCountTracker.entrySet()) {
	        count = entry.getValue().getCurrentValue();
	        word = entry.getKey();
		    frequencyOfWords.put(count, word);
        }
	    final StringBuilder dumpWordsToLog = new StringBuilder();

	    List<String> words;
	    for (final Long key: frequencyOfWords.keySet()) {
		    if (minWordCountThreshold < key) {
			    words = (List<String>) frequencyOfWords.get(key);
			    Collections.sort(words);
			    dumpWordsToLog.append(key)
					    .append(" ==> ")
					    .append(words)
					    .append("\n");
		    }
	    }
	    LOGGER.info("At {}, total # of words received in run#{}: {} ", new Date(), runCounter, wordCountTracker.size());
	    LOGGER.info("\n{}", dumpWordsToLog.toString());

	    // Empty frequency and wordCountTracker Maps for further iterations.
        wordCountTracker.clear();
	    frequencyOfWords.clear();
    }

	/**
	 * Just dint feel like using HashMap.get() and then increment the variable.
	 * Hence used this piece to do the same in a different fashion. :-)
	 */
	private final class MutableLong {
		private long count = 1L;

		public final void increment() {
			++count;
		}

		public final long getCurrentValue() {
			return count;
		}
	}
}