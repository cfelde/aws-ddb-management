/*
    AWS DynamoDB table throughput management
    Copyright (C) 2013  Christian Felde

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.cfelde.aws.ddb.management;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * An instance of a TableThroughput is responsible for scaling read and write
 * throughput up and down based on current load in a AWS DynamoDB table.
 *
 * @author Christian Felde
 */
public class TableThroughput {
    private static final Log LOG = LogFactory.getLog(TableThroughput.class);

    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private static final Object lock = new Object();

    private static final File stateFile = new File(".tablePartitionState");
    private static volatile Map<String, List<Double>> allPartitionEstimators = new HashMap<>();

    private final AmazonCloudWatch client;
    private final AmazonDynamoDBClient ddb;
    private final String tableName;
    private final int minReadLimit, maxReadLimit;
    private final int minWriteLimit, maxWriteLimit;

    // Measurement period in minutes
    private final int period = 60 * 5;

    // Throttled multiplier
    private final double throttledMultiplier = 2D;

    private final int maxConsumedCount = 3;
    private final List<Double> consumedReadValues = new ArrayList<>();
    private final List<Double> consumedWriteValues = new ArrayList<>();

    private final int estimatorCounts = 15;
    private final List<Double> readPartitionEstimators;
    private final List<Double> writePartitionEstimators;

    private final AtomicLong downscaleCounter = new AtomicLong(4);
    private final AtomicInteger exceptionCounter = new AtomicInteger();

    private final AtomicLong requestedReadCapacity = new AtomicLong();
    private final AtomicLong requestedWriteCapacity = new AtomicLong();

    private volatile Double lastReadCapacity, lastWriteCapacity;
    private volatile DateTime lastReadChange = new DateTime().minusDays(1);
    private volatile DateTime lastWriteChange = new DateTime().minusDays(1);
    private volatile DateTime lastTableChange = new DateTime().minusDays(1);

    public static void main(String... args) {
        loadPartitionState();

        // TODO: All these parameters should be loaded from a config file!
        String accessKey = "YOUR ACCESS KEY";
        String secretKey = "YOUR SECRET KEY";

        AmazonCloudWatch client = new AmazonCloudWatchClient(new BasicAWSCredentials(accessKey, secretKey));
        AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(new BasicAWSCredentials(accessKey, secretKey));

        client.setEndpoint("https://monitoring.eu-west-1.amazonaws.com");
        ddb.setEndpoint("https://dynamodb.eu-west-1.amazonaws.com");

        // Do one per table you want to manage
        initTableThroughput(client, ddb, "table1", 2, 100, 2, 100);
        initTableThroughput(client, ddb, "table2", 2, 100, 2, 100);

        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                storePartitionState();
            }
        }, 16, 61, TimeUnit.MINUTES);
    }

    private static void initTableThroughput(AmazonCloudWatch client, AmazonDynamoDBClient ddb, String table, int minRead, int maxRead, int minWrite, int maxWrite) {
        final TableThroughput tt = new TableThroughput(client, ddb, table, minRead, maxRead, minWrite, maxWrite);

        tt.initCapacityValues();

        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                tt.updateDownscaleCounter();
            }
        }, 1, 60 * 15, TimeUnit.SECONDS);

        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                tt.runRead();
                tt.runWrite();
            }
        }, 10, 60 * 5, TimeUnit.SECONDS);

        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                tt.updateCapacity();
            }
        }, 20, 120, TimeUnit.SECONDS);

        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                tt.runExceptionCheck();
            }
        }, 5, 60, TimeUnit.MINUTES);
    }

    private static void loadPartitionState() {
        synchronized (lock) {
            if (stateFile.isFile()) {
                LOG.info("Loading partition state file: " + stateFile.getAbsolutePath());
                try (ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(stateFile)))) {
                    allPartitionEstimators = (Map<String, List<Double>>) ois.readObject();
                } catch (IOException | ClassNotFoundException ex) {
                    LOG.warn("Failed to load existing partition state file: " + ex.getMessage(), ex);
                    stateFile.delete();
                }
            }
        }
    }

    private static void storePartitionState() {
        synchronized (lock) {
            LOG.info("Storing partition state file: " + stateFile.getAbsolutePath());
            try (ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(stateFile)))) {
                oos.writeObject(allPartitionEstimators);
            } catch (IOException ex) {
                LOG.warn("Failed to store partition state file: " + ex.getMessage(), ex);
                stateFile.delete();
            }
        }
    }

    private static void updatePartitionState(String tableAndRW, List<Double> estimates) {
        synchronized (lock) {
            allPartitionEstimators.put(tableAndRW, new ArrayList<>(estimates));
        }
    }

    private static List<Double> getPartitionState(String tableAndRW) {
        synchronized (lock) {
            List<Double> estimates = allPartitionEstimators.get(tableAndRW);
            if (estimates == null) {
                return new ArrayList<>();
            } else {
                return new ArrayList<>(estimates);
            }
        }
    }

    public TableThroughput(AmazonCloudWatch client, AmazonDynamoDBClient ddb, String tableName, int minReadLimit, int maxReadLimit, int minWriteLimit, int maxWriteLimit) {
        this.client = client;
        this.ddb = ddb;
        this.tableName = tableName;
        this.minReadLimit = minReadLimit;
        this.maxReadLimit = maxReadLimit;
        this.minWriteLimit = minWriteLimit;
        this.maxWriteLimit = maxWriteLimit;

        this.readPartitionEstimators = new ArrayList<>(getPartitionState(tableName + "::read"));
        this.writePartitionEstimators = new ArrayList<>(getPartitionState(tableName + "::write"));
    }

    public synchronized void runRead() {
        try {
            if (lastReadChange.plusMinutes(11).isAfter(new DateTime()))
                return;

            LOG.info("Checking reads on " + tableName);

            double readCapacity = getReadCapacity();
            double consumedReadCapacity = getConsumedReadCapacity();
            double throttledReads = getThrottledReads();

            double partitionEstimator = getReadPartitionEstimator();

            LOG.info("Read stats on " + tableName + ": capacity: " + readCapacity + ", consumed: " + consumedReadCapacity + ", throttled: " + throttledReads + ", partition estimator: " + partitionEstimator);

            if (throttledReads > 0.1 || throttledReads > readCapacity / 10) {
                // Scale up case
                addReadPartitionEstimator(readCapacity / consumedReadCapacity);

                if (readCapacity < maxReadLimit) {
                    LOG.info("Increasing read capacity on " + tableName);
                    double change = (throttledReads * throttledMultiplier);
                    if (change < readCapacity / 2)
                        change = readCapacity / 2;
                    if (change < minReadLimit)
                        change = minReadLimit;
                    if (setReadCapacity(change + readCapacity))
                        lastReadChange = new DateTime();
                }
            } else if (throttledReads > 0) {
                // Ignore case
                LOG.info("Read capacity within tolerance on " + tableName);
            } else if (consumedReadCapacity * partitionEstimator < readCapacity / 2) {
                // Scale down case
                if (readCapacity > minReadLimit && isDownscaleAllowed()) {
                    LOG.info("Decreasing read capacity on " + tableName);
                    if (setReadCapacity(consumedReadCapacity * partitionEstimator))
                        lastReadChange = new DateTime();
                }
            }
        } catch (Throwable t) {
            LOG.warn("Exception in runRead: " + t.getMessage(), t);
            exceptionCounter.incrementAndGet();
        }
    }

    public synchronized void runWrite() {
        try {
            if (lastWriteChange.plusMinutes(11).isAfter(new DateTime()))
                return;

            LOG.info("Checking writes on " + tableName);

            double writeCapacity = getWriteCapacity();
            double consumedWriteCapacity = getConsumedWriteCapacity();
            double throttledWrites = getThrottledWrites();

            double partitionEstimator = getWritePartitionEstimator();

            LOG.info("Write stats on " + tableName + ": capacity: " + writeCapacity + ", consumed: " + consumedWriteCapacity + ", throttled: " + throttledWrites + ", partition estimator: " + partitionEstimator);

            if (throttledWrites > 0.1 || throttledWrites > writeCapacity / 10) {
                // Scale up case
                addWritePartitionEstimator(writeCapacity / consumedWriteCapacity);

                if (writeCapacity < maxWriteLimit) {
                    LOG.info("Increasing write capacity on " + tableName);
                    double change = (throttledWrites * throttledMultiplier);
                    if (change < writeCapacity / 2)
                        change = writeCapacity / 2;
                    if (change < minWriteLimit)
                        change = minWriteLimit;
                    if (setWriteCapacity(change + writeCapacity))
                        lastWriteChange = new DateTime();
                }
            } else if (throttledWrites > 0) {
                // Ignore case
                LOG.info("Write capacity within tolerance on " + tableName);
            } else if (consumedWriteCapacity * partitionEstimator < writeCapacity / 2) {
                // Scale down case
                if (writeCapacity > minWriteLimit && isDownscaleAllowed()) {
                    LOG.info("Decreasing write capacity on " + tableName);
                    if (setWriteCapacity(consumedWriteCapacity * partitionEstimator))
                        lastWriteChange = new DateTime();
                }
            }
        } catch (Throwable t) {
            LOG.warn("Exception in runWrite: " + t.getMessage(), t);
            exceptionCounter.incrementAndGet();
        }
    }

    public void runExceptionCheck() {
        int count = exceptionCounter.getAndSet(0);

        if (count > 10) {
            LOG.info("Too many exceptions (" + count + "), shutting down..");
            executor.shutdown();
            System.exit(1);
        }
    }

    private void addReadPartitionEstimator(double partitions) {
        readPartitionEstimators.add(partitions);

        while (readPartitionEstimators.size() > estimatorCounts)
            readPartitionEstimators.remove(0);

        updatePartitionState(tableName + "::read", readPartitionEstimators);
    }

    private double getReadPartitionEstimator() {
        List<Double> values = new ArrayList<>(readPartitionEstimators);
        Collections.sort(values);

        if (values.size() < 3)
            return 1;

        values.remove(0);
        values.remove(values.size()-1);

        double sum = 0;

        for (Double pe: values)
            sum += pe;

        return values.size() < 3 ? 1 : sum / values.size();
    }

    private void addWritePartitionEstimator(double partitions) {
        writePartitionEstimators.add(partitions);

        while (writePartitionEstimators.size() > estimatorCounts)
            writePartitionEstimators.remove(0);

        updatePartitionState(tableName + "::write", writePartitionEstimators);
    }

    private double getWritePartitionEstimator() {
        List<Double> values = new ArrayList<>(writePartitionEstimators);
        Collections.sort(values);

        if (values.size() < 3)
            return 1;

        values.remove(0);
        values.remove(values.size()-1);

        double sum = 0;

        for (Double pe: values)
            sum += pe;

        return values.size() < 3 ? 1 : sum / values.size();
    }

    public boolean setReadCapacity(double capacity) {
        return Math.round(capacity) != requestedReadCapacity.getAndSet(Math.round(capacity));
    }

    public boolean setWriteCapacity(double capacity) {
        return Math.round(capacity) != requestedWriteCapacity.getAndSet(Math.round(capacity));
    }

    public boolean isDownscaleAllowed() {
        // We allow 4 downscales within 24 hours:

        // Between 00:00 and 06:00 - 3 left after
        // Between 06:00 and 12:00 - 2 left after
        // Between 12:00 and 18:00 - 1 left after
        // Between 18:00 and 00:00 - 0 left after

        // First, based on current UTC time, find the number of
        // downscales we at most can use.
        DateTime dtNow = new DateTime(DateTimeZone.UTC);

        int hourNow = dtNow.getHourOfDay();
        long maxDownscales;
        if (hourNow >= 18)
            maxDownscales = 4;
        else if (hourNow >= 12)
            maxDownscales = 3;
        else if (hourNow >= 6)
            maxDownscales = 2;
        else
            maxDownscales = 1;

        long usedDownscaled = downscaleCounter.get();

        return usedDownscaled < maxDownscales;
    }

    public void initCapacityValues() {
        synchronized (lock) {
            try {
                ProvisionedThroughputDescription ptd = ddb.describeTable(new DescribeTableRequest(tableName))
                        .getTable().getProvisionedThroughput();

                requestedReadCapacity.compareAndSet(0, ptd.getReadCapacityUnits());
                requestedWriteCapacity.compareAndSet(0, ptd.getWriteCapacityUnits());

                LOG.info("Initial capacity on " + tableName + ": reads: " + ptd.getReadCapacityUnits() + ", writes: " + ptd.getWriteCapacityUnits());
            } catch (Throwable t) {
                LOG.error("Exception in initCapacityValues: " + t.getMessage(), t);
                exceptionCounter.incrementAndGet();
            }
        }
    }

    public void updateDownscaleCounter() {
        synchronized (lock) {
            try {
                ProvisionedThroughputDescription ptd = ddb.describeTable(new DescribeTableRequest(tableName))
                        .getTable().getProvisionedThroughput();

                downscaleCounter.set(ptd.getNumberOfDecreasesToday());

                LOG.info("Current scale down counter value on " + tableName + ": " + ptd.getNumberOfDecreasesToday());
            } catch (Throwable t) {
                LOG.error("Exception in updateDownscaleCounter: " + t.getMessage(), t);
                exceptionCounter.incrementAndGet();
            }
        }
    }

    public void updateCapacity() {
        synchronized (lock) {
            try {
                if (lastTableChange.plusMinutes(3).isAfter(new DateTime()))
                    return;

                long readCapacity = requestedReadCapacity.get();
                long writeCapacity = requestedWriteCapacity.get();

                if (readCapacity > maxReadLimit)
                    readCapacity = maxReadLimit;
                if (readCapacity < minReadLimit)
                    readCapacity = minReadLimit;

                if (writeCapacity > maxWriteLimit)
                    writeCapacity = maxWriteLimit;
                if (writeCapacity < minWriteLimit)
                    writeCapacity = minWriteLimit;

                ProvisionedThroughputDescription ptd = ddb.describeTable(new DescribeTableRequest(tableName))
                        .getTable().getProvisionedThroughput();

                downscaleCounter.set(ptd.getNumberOfDecreasesToday());

                final long currentReadCapacity = ptd.getReadCapacityUnits();
                final long currentWriteCapacity = ptd.getWriteCapacityUnits();

                // Make sure we don't try to scale up more than 100%
                if (readCapacity > currentReadCapacity * 2)
                    readCapacity = currentReadCapacity * 2;

                if (writeCapacity > currentWriteCapacity * 2)
                    writeCapacity = currentWriteCapacity * 2;

                if (!isDownscaleAllowed() && readCapacity < currentReadCapacity)
                    readCapacity = currentReadCapacity;
                if (!isDownscaleAllowed() && writeCapacity < currentWriteCapacity)
                    writeCapacity = currentWriteCapacity;

                // Check if no change
                if (readCapacity == currentReadCapacity && writeCapacity == currentWriteCapacity)
                    return;

                /*
                if (readCapacity < currentReadCapacity || writeCapacity < currentWriteCapacity)
                    downscaleAllowed.set(false);
                */

                ProvisionedThroughput throughput = new ProvisionedThroughput();
                throughput.withReadCapacityUnits(readCapacity);
                throughput.withWriteCapacityUnits(writeCapacity);

                UpdateTableRequest request = new UpdateTableRequest(tableName, throughput);

                LOG.info("Changing throughput on " + tableName + " to: reads: " + throughput.getReadCapacityUnits() + ", writes: " + throughput.getWriteCapacityUnits());
                ddb.updateTable(request);

                lastTableChange = new DateTime();
            } catch (Throwable t) {
                LOG.error("Exception in updateCapacity: " + t.getMessage(), t);
                exceptionCounter.incrementAndGet();
            }
        }
    }

    /**
     * Returns number of reads per second.
     *
     * @return Reads per second
     */
    public double getReadCapacity() {
        synchronized (lock) {
            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
            request.setStatistics(Collections.singleton("Sum"));
            request.setDimensions(Arrays.asList(
                    new Dimension().withName("TableName").withValue(tableName)));
            request.setNamespace("AWS/DynamoDB");
            request.setStartTime(new Date(System.currentTimeMillis() - (1000L * period * 5)));
            request.setEndTime(new Date(System.currentTimeMillis() + 60000L));
            request.setPeriod(period);
            request.setMetricName("ProvisionedReadCapacityUnits");

            GetMetricStatisticsResult result = client.getMetricStatistics(request);

            if (result.getDatapoints().isEmpty())
                return lastReadCapacity == null ? 0 : lastReadCapacity;

            List<Datapoint> dataPoints = new ArrayList<>(result.getDatapoints());

            Collections.sort(dataPoints, new Comparator<Datapoint>() {
                @Override
                public int compare(Datapoint o1, Datapoint o2) {
                    return o2.getTimestamp().compareTo(o1.getTimestamp());
                }
            });

            Datapoint datapoint = dataPoints.get(0);

            lastReadCapacity = datapoint.getSum();

            return lastReadCapacity;
        }
    }

    /**
     * Returns number of writes per second.
     *
     * @return Reads per second
     */
    public double getWriteCapacity() {
        synchronized (lock) {
            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
            request.setStatistics(Collections.singleton("Sum"));
            request.setDimensions(Arrays.asList(
                    new Dimension().withName("TableName").withValue(tableName)));
            request.setNamespace("AWS/DynamoDB");
            request.setStartTime(new Date(System.currentTimeMillis() - (1000L * period * 5)));
            request.setEndTime(new Date(System.currentTimeMillis() + 60000L));
            request.setPeriod(period);
            request.setMetricName("ProvisionedWriteCapacityUnits");

            GetMetricStatisticsResult result = client.getMetricStatistics(request);

            if (result.getDatapoints().isEmpty())
                return lastWriteCapacity == null ? 0 : lastWriteCapacity;

            List<Datapoint> dataPoints = new ArrayList<>(result.getDatapoints());

            Collections.sort(dataPoints, new Comparator<Datapoint>() {
                @Override
                public int compare(Datapoint o1, Datapoint o2) {
                    return o2.getTimestamp().compareTo(o1.getTimestamp());
                }
            });

            Datapoint datapoint = dataPoints.get(0);

            lastWriteCapacity = datapoint.getSum();

            return lastWriteCapacity;
        }
    }

    /**
     * Returns number of consumed reads per second.
     *
     * @return Reads per second
     */
    public double getConsumedReadCapacity() {
        synchronized (lock) {
            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
            request.setStatistics(Collections.singleton("Sum"));
            request.setDimensions(Arrays.asList(
                    new Dimension().withName("TableName").withValue(tableName)));
            request.setNamespace("AWS/DynamoDB");
            request.setStartTime(new Date(System.currentTimeMillis() - (1000L * period * 5)));
            request.setEndTime(new Date(System.currentTimeMillis() + 60000L));
            request.setPeriod(period);
            request.setMetricName("ConsumedReadCapacityUnits");

            GetMetricStatisticsResult result = client.getMetricStatistics(request);

            if (!result.getDatapoints().isEmpty()) {
                List<Datapoint> dataPoints = new ArrayList<>(result.getDatapoints());

                Collections.sort(dataPoints, new Comparator<Datapoint>() {
                    @Override
                    public int compare(Datapoint o1, Datapoint o2) {
                        return o2.getTimestamp().compareTo(o1.getTimestamp());
                    }
                });

                Datapoint datapoint = dataPoints.get(0);

                consumedReadValues.add(datapoint.getSum() / period);
            } else {
                consumedReadValues.add(0D);
            }

            while (consumedReadValues.size() > maxConsumedCount)
                consumedReadValues.remove(0);

            if (consumedReadValues.isEmpty())
                return 0;

            double maxConsumedValue = Double.MIN_VALUE;
            for (Double c: consumedReadValues) {
                if (c > maxConsumedValue)
                    maxConsumedValue = c;
            }

            return maxConsumedValue;
        }
    }

    /**
     * Returns number of consumed write per second.
     *
     * @return Reads per second
     */
    public double getConsumedWriteCapacity() {
        synchronized (lock) {
            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
            request.setStatistics(Collections.singleton("Sum"));
            request.setDimensions(Arrays.asList(
                    new Dimension().withName("TableName").withValue(tableName)));
            request.setNamespace("AWS/DynamoDB");
            request.setStartTime(new Date(System.currentTimeMillis() - (1000L * period * 5)));
            request.setEndTime(new Date(System.currentTimeMillis() + 60000L));
            request.setPeriod(period);
            request.setMetricName("ConsumedWriteCapacityUnits");

            GetMetricStatisticsResult result = client.getMetricStatistics(request);

            if (!result.getDatapoints().isEmpty()) {
                List<Datapoint> dataPoints = new ArrayList<>(result.getDatapoints());

                Collections.sort(dataPoints, new Comparator<Datapoint>() {
                    @Override
                    public int compare(Datapoint o1, Datapoint o2) {
                        return o2.getTimestamp().compareTo(o1.getTimestamp());
                    }
                });

                Datapoint datapoint = dataPoints.get(0);

                consumedWriteValues.add(datapoint.getSum() / period);
            } else {
                consumedWriteValues.add(0D);
            }

            while (consumedWriteValues.size() > maxConsumedCount)
                consumedWriteValues.remove(0);

            if (consumedWriteValues.isEmpty())
                return 0;

            double maxConsumedValue = Double.MIN_VALUE;
            for (Double c: consumedWriteValues) {
                if (c > maxConsumedValue)
                    maxConsumedValue = c;
            }

            return maxConsumedValue;
        }
    }

    /**
     * Returns number of throttled reads per second.
     *
     * @return Reads per second
     */
    public double getThrottledReads() {
        synchronized (lock) {
            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
            request.setStatistics(Collections.singleton("Sum"));
            request.setDimensions(Arrays.asList(
                    new Dimension().withName("TableName").withValue(tableName)));
            request.setNamespace("AWS/DynamoDB");
            request.setStartTime(new Date(System.currentTimeMillis() - (1000L * period * 5)));
            request.setEndTime(new Date(System.currentTimeMillis() + 60000L));
            request.setPeriod(period);
            request.setMetricName("ReadThrottleEvents");

            GetMetricStatisticsResult result = client.getMetricStatistics(request);

            if (result.getDatapoints().isEmpty())
                return 0;

            List<Datapoint> dataPoints = new ArrayList<>(result.getDatapoints());

            Collections.sort(dataPoints, new Comparator<Datapoint>() {
                @Override
                public int compare(Datapoint o1, Datapoint o2) {
                    return o2.getTimestamp().compareTo(o1.getTimestamp());
                }
            });

            Datapoint datapoint = dataPoints.get(0);

            // Do this to ensure the additive nature of how we use it works
            if (datapoint.getTimestamp().getTime() < lastReadChange.getMillis())
                return 0;

            return datapoint.getSum() / period;
        }
    }

    /**
     * Returns number of throttled writes per second.
     *
     * @return Reads per second
     */
    public double getThrottledWrites() {
        synchronized (lock) {
            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
            request.setStatistics(Collections.singleton("Sum"));
            request.setDimensions(Arrays.asList(
                    new Dimension().withName("TableName").withValue(tableName)));
            request.setNamespace("AWS/DynamoDB");
            request.setStartTime(new Date(System.currentTimeMillis() - (1000L * period * 5)));
            request.setEndTime(new Date(System.currentTimeMillis() + 60000L));
            request.setPeriod(period);
            request.setMetricName("WriteThrottleEvents");

            GetMetricStatisticsResult result = client.getMetricStatistics(request);

            if (result.getDatapoints().isEmpty())
                return 0;

            List<Datapoint> dataPoints = new ArrayList<>(result.getDatapoints());

            Collections.sort(dataPoints, new Comparator<Datapoint>() {
                @Override
                public int compare(Datapoint o1, Datapoint o2) {
                    return o2.getTimestamp().compareTo(o1.getTimestamp());
                }
            });

            Datapoint datapoint = dataPoints.get(0);

            // Do this to ensure the additive nature of how we use it works
            if (datapoint.getTimestamp().getTime() < lastWriteChange.getMillis())
                return 0;

            return datapoint.getSum() / period;
        }
    }
}
