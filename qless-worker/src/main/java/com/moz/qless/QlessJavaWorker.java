package com.moz.qless;

import java.io.IOException;
import java.util.Arrays;

import com.moz.qless.workers.SerialWorker;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import redis.clients.jedis.JedisPool;

public class QlessJavaWorker {
  @Option(name = "-host", usage = "The url to connect to Redis")
  public String host = "localhost";

  @Option(name = "-name", usage = "The hostname to identify your worker as")
  public String name = null;

  @Option(name = "-interval", usage = "The pulling interval")
  public int inverval = 60;

  @Option(name = "-queue", required = true, handler = StringArrayOptionHandler.class,
      usage = "The queues to pull work from")
  public String[] queues = null;

  public static void main(final String[] args) {
    final QlessJavaWorker worker = new QlessJavaWorker();
    final CmdLineParser parser = new CmdLineParser(worker);

    try {
      parser.parseArgument(args);
    } catch (final CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
    }

    try {
      final JedisPool jedisPool = new JedisPool(worker.host);
      final Client client = new Client(jedisPool);

      final SerialWorker serialWorker = new SerialWorker(
          Arrays.asList(worker.queues),
          client,
          worker.name,
          worker.inverval);

      serialWorker.run();
    } catch (InterruptedException | IOException e) {
      System.err.println(e.getMessage());
    }
  }
}
