package org.p7h.storm.wordcount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.p7h.storm.wordcount.bolts.WordCountBolt;
import org.p7h.storm.wordcount.bolts.WordSplitBolt;
import org.p7h.storm.wordcount.spouts.TwitterSpout;
import org.p7h.storm.wordcount.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the elements and forms a Topology to count the words present in Tweets.
 *
 * @author - Prashanth Babu
 */
public final class WordCountTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTopology.class);

	public static final void main(final String[] args) {
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			final TopologyBuilder topologyBuilder = new TopologyBuilder();
			topologyBuilder.setSpout("twitterspout", new TwitterSpout());
			//Create WordSplitBolt with minimum word length to be considered.
			//This is more to reduce the number of words to be processed i.e. for ignoring simple and most used words.
			topologyBuilder.setBolt("wordsplitbolt", new WordSplitBolt(4))
					.shuffleGrouping("twitterspout");
			//Create Bolt with the frequency of logging [in seconds] and count threshold of words.
			topologyBuilder.setBolt("wordcountbolt", new WordCountBolt(30, 9))
					.shuffleGrouping("wordsplitbolt");

			//Submit it to the cluster, or submit it locally
			if (null != args && 0 < args.length) {
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			} else {
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(Constants.TOPOLOGY_NAME, config, topologyBuilder.createTopology());
				//Run this topology for 120 seconds so that we can complete processing of decent # of tweets.
				Utils.sleep(120 * 1000);

				LOGGER.info("Shutting down the cluster...");
				localCluster.killTopology(Constants.TOPOLOGY_NAME);
				localCluster.shutdown();
			}
		} catch (final AlreadyAliveException | InvalidTopologyException exception) {
			//Deliberate no op; not required actually.
			//exception.printStackTrace();
		} catch (final Exception exception) {
			//Deliberate no op; not required actually.
			//exception.printStackTrace();
		}
		LOGGER.info("\n\n\n\t\t*****Please clean your temp folder \"{}\" now!!!*****", System.getProperty("java.io.tmpdir"));
	}
}