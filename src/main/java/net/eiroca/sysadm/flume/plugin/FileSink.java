/**
 *
 * Copyright (C) 1999-2020 Enrico Croce - AGPL >= 3.0
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package net.eiroca.sysadm.flume.plugin;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.PathManagerFactory;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.LongParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;

public class FileSink extends AbstractSink implements Configurable, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(FileSink.class);

  final Parameters params = new Parameters();
  final LongParameter pBatchSize = new LongParameter(params, "batch-size", 100);
  final StringParameter pSerializerType = new StringParameter(params, "serializer", null);
  final StringParameter pPathManagerType = new StringParameter(params, "sink.pathManager", "DEFAULT");
  final StringParameter pDirectory = new StringParameter(params, "sink.pathManager.directory");
  final IntegerParameter pRollInterval = new IntegerParameter(params, "sink.pathManager.roll-interval", 30);
  final IntegerParameter pRollSize = new IntegerParameter(params, "sink.pathManager.roll-size", -1);

  private File directory;
  private int batchSize;

  private OutputStream outputStream;
  private ScheduledExecutorService rollService;

  private String serializerType;
  private Context serializerContext;
  private EventSerializer serializer;

  private SinkCounter sinkCounter;

  protected PathManager pathController;
  protected volatile boolean shouldRotate;

  protected int rollInterval = -1;
  protected int rollSize = -1;

  public FileSink() {
    shouldRotate = false;
  }

  @Override
  public void configure(final Context context) {
    FlumeHelper.laodConfig(params, context);
    final String pathManagerType = pPathManagerType.get();
    final String directory = pDirectory.get();
    rollInterval = pRollInterval.get().intValue();
    rollSize = pRollSize.get();
    if (rollSize > 0) {
      rollSize *= 1024 * 1024;
    }
    batchSize = pBatchSize.get().intValue();
    serializerType = pSerializerType.get();
    serializerContext = new Context(context.getSubProperties("sink." + EventSerializer.CTX_PREFIX));
    final Context pathManagerContext = new Context(context.getSubProperties("sink." + PathManager.CTX_PREFIX));
    pathController = PathManagerFactory.getInstance(pathManagerType, pathManagerContext);
    this.directory = new File(directory);
    try {
      if (!this.directory.exists()) {
        this.directory.mkdir();
      }
    }
    catch (final Exception e) {
      FileSink.logger.error("Unable to create directory {}", directory, e);
      throw e;
    }
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public void start() {
    FileSink.logger.info("Starting {}...", this);
    sinkCounter.start();
    super.start();
    pathController.setBaseDirectory(directory);
    if (rollInterval > 0) {
      /*
       * Every N seconds, mark that it's time to rotate. We purposefully do NOT touch anything other
       * than the indicator flag to avoid error handling issues (e.g. IO exceptions occurring in two
       * different threads. Resist the urge to actually perform rotation in a separate thread!
       */
      rollService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("FileSink-roller-" + Thread.currentThread().getId() + "-%d").build());
      rollService.scheduleAtFixedRate(this, rollInterval, rollInterval, TimeUnit.SECONDS);
    }
    else {
      FileSink.logger.info("RollInterval is not valid, file rolling will not happen.");
    }
    FileSink.logger.info("FileSink {} started.", getName());
  }

  private boolean closeFile() {
    boolean result = true;
    if (outputStream != null) {
      FileSink.logger.debug("Closing file {}", pathController.getCurrentFile());
      try {
        serializer.flush();
        serializer.beforeClose();
        outputStream.close();
        sinkCounter.incrementConnectionClosedCount();
      }
      catch (final IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        FileSink.logger.error("Unable to close output stream. Exception follows.", e);
        result = false;
      }
      finally {
        serializer = null;
        outputStream = null;
      }
    }
    return result;
  }

  public File rotate(final File currentFile) throws EventDeliveryException {
    FileSink.logger.debug("Rotating {}", currentFile);
    if (!closeFile()) { throw new EventDeliveryException("Unable to rotate file " + currentFile + " while delivering event"); }
    shouldRotate = false;
    pathController.rotate();
    return pathController.getCurrentFile();
  }

  @Override
  public Status process() throws EventDeliveryException {
    File currentFile = pathController.getCurrentFile();
    final File oldFile = currentFile;
    if (shouldRotate) {
      currentFile = rotate(currentFile);
    }
    if (outputStream == null) {
      FileSink.logger.debug("Opening output stream for file {}", currentFile);
      try {
        outputStream = new BufferedOutputStream(new FileOutputStream(currentFile));
        serializer = EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);
        serializer.afterCreate();
        sinkCounter.incrementConnectionCreatedCount();
      }
      catch (final IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        throw new EventDeliveryException("Failed to open file " + pathController.getCurrentFile() + " while delivering event", e);
      }
    }
    final Channel channel = getChannel();
    final Transaction transaction = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;
    try {
      transaction.begin();
      int eventAttemptCounter = 0;
      for (int i = 0; i < batchSize; i++) {
        event = channel.take();
        if (event != null) {
          sinkCounter.incrementEventDrainAttemptCount();
          eventAttemptCounter++;
          serializer.write(event);
        }
        else {
          // No events found, request back-off semantics from runner
          result = Status.BACKOFF;
          break;
        }
      }
      serializer.flush();
      outputStream.flush();
      transaction.commit();
      sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
    }
    catch (final Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to process transaction", ex);
    }
    finally {
      transaction.close();
    }
    final File newFile = pathController.getCurrentFile();
    if (!oldFile.equals(newFile)) {
      if (oldFile.length() == 0) {
        try {
          oldFile.delete();
        }
        catch (final Exception e) {
          FileSink.logger.warn("Could not delete potential empty file", e);
        }
      }
    }
    else if ((rollSize > 0) && (oldFile.length() > rollSize)) {
      shouldRotate = true; // force rotate due to size
    }
    return result;
  }

  @Override
  public void stop() {
    FileSink.logger.info("File sink {} stopping...", getName());
    final File oldFile = pathController.getCurrentFile();
    sinkCounter.stop();
    super.stop();
    closeFile();
    if (rollInterval > 0) {
      rollService.shutdown();
      while (!rollService.isTerminated()) {
        try {
          rollService.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (final InterruptedException e) {
          FileSink.logger.debug("Interrupted while waiting for roll service to stop. Please report this.", e);
        }
      }
    }
    if ((oldFile != null) && oldFile.exists() && (oldFile.length() == 0)) {
      oldFile.delete();
    }
    FileSink.logger.info("File sink {} stopped. Event metrics: {}", getName(), sinkCounter);
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(final File directory) {
    this.directory = directory;
  }

  public long getRollInterval() {
    return rollInterval;
  }

  public void setRollInterval(final int rollInterval) {
    this.rollInterval = rollInterval;
  }

  @Override
  public void run() {
    FileSink.logger.debug("Marking time to rotate file {}", pathController.getCurrentFile());
    shouldRotate = true;
  }

}
