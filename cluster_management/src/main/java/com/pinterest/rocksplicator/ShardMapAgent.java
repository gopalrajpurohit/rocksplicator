package com.pinterest.rocksplicator;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardMapAgent {
  private static final Logger LOG = LoggerFactory.getLogger(ShardMapAgent.class);

  private static final String zkShardMapSvrArg = "zkShardMapSvr";
  private static final String clusterArg = "cluster";
  private static final String clustersFileArg  = "clustersFile";
  private static final String shardMapDirArg = "shardMapDir";

  private static Options constructCommandLineOptions() {
    Option zkShardMapSvrOption = OptionBuilder
        .withLongOpt(zkShardMapSvrArg)
        .withDescription("Provide zk server connect string hosting the shard_maps [Required]").create();
    zkShardMapSvrOption.setArgs(1);
    zkShardMapSvrOption.setRequired(true);
    zkShardMapSvrOption.setArgName(zkShardMapSvrArg);

    Option clusterOption = OptionBuilder
        .withLongOpt(clusterArg)
        .withDescription("Provide cluster to download shard_map for [Optional]").create();
    zkShardMapSvrOption.setArgs(1);
    zkShardMapSvrOption.setRequired(false);
    zkShardMapSvrOption.setArgName(clusterArg);

    Option clustersFileOption = OptionBuilder
        .withLongOpt(clustersFileArg)
        .withDescription("Provide file path containing clusters to download shard_maps [Optional,"
            + " at least one of cluster or clustersFile argument must be provided ]").create();
    zkShardMapSvrOption.setArgs(1);
    zkShardMapSvrOption.setRequired(false);
    zkShardMapSvrOption.setArgName(clustersFileArg);

    Option shardMapDirOption = OptionBuilder
        .withLongOpt(shardMapDirArg)
        .withDescription("Provide directory to download shardMap for each cluster").create();
    zkShardMapSvrOption.setArgs(1);
    zkShardMapSvrOption.setRequired(true);
    zkShardMapSvrOption.setArgName(shardMapDirArg);

    Options options = new Options();
    options.addOption(zkShardMapSvrOption)
        .addOption(clusterOption)
        .addOption(clustersFileOption)
        .addOption(shardMapDirOption);

    return options;
  }

  public static void main(String[] args) {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.WARN);
    BasicConfigurator.configure(new ConsoleAppender(
        new PatternLayout("%d{HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n")
    ));
  }
}
