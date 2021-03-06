package com.moz.qless;

import java.io.IOException;
import java.util.Arrays;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

public class Worker {
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
    final Worker worker = new Worker();
    final CmdLineParser parser = new CmdLineParser(worker);

    try {
      parser.parseArgument(args);
    } catch (final CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
    }

    try {
      final Client client = Client.builder()
        .jedisUri(worker.host)
        .workerName(worker.name)
        .build();

      final SerialWorker serialWorker = new SerialWorker(
          Arrays.asList(worker.queues),
          client,
          worker.inverval);

      serialWorker.run();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }
}
