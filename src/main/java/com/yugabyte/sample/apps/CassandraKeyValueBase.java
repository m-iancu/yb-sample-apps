package com.yugabyte.sample.apps;

import com.datastax.driver.core.*;
import com.yugabyte.sample.common.SimpleLoadGenerator;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for all workloads that are based on key value tables.
 */
public abstract class CassandraKeyValueBase extends AppBase {
  private static final Logger LOG = Logger.getLogger(CassandraKeyValueBase.class);

  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 1 each.
    appConfig.numReaderThreads = 24;
    appConfig.numWriterThreads = 2;
    // The number of keys to read.
    appConfig.numKeysToRead = -1;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = -1;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
  }

  // The shared prepared select statement for fetching the data.
  private static volatile PreparedStatement preparedSelect;

  // The shared prepared statement for inserting into the table.
  private static volatile PreparedStatement preparedInsert;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  private Map<SimpleLoadGenerator.Key, ResultSetFuture> outstandingAsyncReadRequests =
          new HashMap<>();
  private Map<SimpleLoadGenerator.Key, ResultSetFuture> outstandingAsyncWriteRequests =
          new HashMap<>();

  public CassandraKeyValueBase() {
    buffer = new byte[appConfig.valueSize];
  }

  protected Cluster localCassandraCluster = null;
  protected Session localCassandraSession = null;
  protected PreparedStatement localPreparedInsert = null;
  protected PreparedStatement localPreparedSelect = null;

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() {
    dropCassandraTable(getTableName());
  }

  protected PreparedStatement getPreparedSelect(String selectStmt, boolean localReads)  {
    if (appConfig.useAsyncExecute) {
      if (localCassandraCluster == null) {
        localCassandraCluster = createClusterBuilder(configuration.getContactPoints()).build();
        localCassandraSession = localCassandraCluster.connect();
        // Will create keyspace with "if not exists" option.
        createKeyspace(localCassandraSession, keyspace);
      }
      if (localPreparedSelect == null) {
        localPreparedSelect = localCassandraSession.prepare(selectStmt);
      }
      return localPreparedSelect;
    } else {
      if (preparedSelect == null) {
        synchronized (prepareInitLock) {
          if (preparedSelect == null) {
            // Create the prepared statement object.
            preparedSelect = getCassandraClient().prepare(selectStmt);
            if (localReads) {
              LOG.debug("Doing local reads");
              preparedSelect.setConsistencyLevel(ConsistencyLevel.ONE);
            }
          }
        }
      }
      return preparedSelect;
    }
  }

  protected abstract String getDefaultTableName();

  public String getTableName() {
    return appConfig.tableName != null ? appConfig.tableName : getDefaultTableName();
  }

  @Override
  public synchronized void resetClients() {
    synchronized (prepareInitLock) {
      preparedInsert = null;
      preparedSelect = null;
    }
    super.resetClients();
  }

  @Override
  public synchronized void destroyClients() {
    synchronized (prepareInitLock) {
      preparedInsert = null;
      preparedSelect = null;
    }
    super.destroyClients();
  }

  protected abstract BoundStatement bindSelect(String key);

  @Override
  public long doRead() {
    SimpleLoadGenerator.Key key = getSimpleLoadGenerator().getKeyToRead();

    if (key == null) {
      // There are no keys to read yet.
      return 0;
    }
    int read_count = 0;

    if (appConfig.useAsyncExecute) {
      if (outstandingAsyncReadRequests.size() >= appConfig.maxAsyncQueueSize) {
        // LOG.info("Got here: " + outstandingAsyncReadRequests.size());
        for (Map.Entry<SimpleLoadGenerator.Key, ResultSetFuture> entry :
                outstandingAsyncReadRequests.entrySet()) {
          try {
            ResultSet rs = entry.getValue().get();
            List<Row> rows = rs.all();
            if (rows.size() != 1) {
              // If TTL is enabled, turn off correctness validation.
              if (appConfig.tableTTLSeconds <= 0) {
                LOG.fatal("Read key: " + entry.getKey().asString() + " expected 1 row in result, got " + rows.size());
              }
              return read_count;
            }
            if (appConfig.valueSize == 0) {
              ByteBuffer buf = rows.get(0).getBytes(1);
              String value = new String(buf.array());
              entry.getKey().verify(value);
            } else {
              ByteBuffer value = rows.get(0).getBytes(1);
              byte[] bytes = new byte[value.capacity()];
              value.get(bytes);
              verifyRandomValue(entry.getKey(), bytes);
            }
            LOG.debug("Read key: " + entry.getKey().toString());
            read_count++;
          } catch (Exception e) {
            LOG.error("Got exception: " + e.getMessage());
          }
        }
        outstandingAsyncReadRequests.clear();
      }
    }

    // Do the read from Cassandra.
    // Bind the select statement.
    BoundStatement select = bindSelect(key.asString());
    if (appConfig.useAsyncExecute) {
      outstandingAsyncReadRequests.put(key, localCassandraSession.executeAsync(select));
      return read_count;
    } else {
      ResultSet rs = getCassandraClient().execute(select);
      List<Row> rows = rs.all();
      if (rows.size() != 1) {
        // If TTL is enabled, turn off correctness validation.
        if (appConfig.tableTTLSeconds <= 0) {
          LOG.fatal("Read key: " + key.asString() + " expected 1 row in result, got " + rows.size());
        }
        return 1;
      }
      if (appConfig.valueSize == 0) {
        ByteBuffer buf = rows.get(0).getBytes(1);
        String value = new String(buf.array());
        key.verify(value);
      } else {
        ByteBuffer value = rows.get(0).getBytes(1);
        byte[] bytes = new byte[value.capacity()];
        value.get(bytes);
        verifyRandomValue(key, bytes);
      }
      LOG.debug("Read key: " + key.toString());
      return 1;
    }
  }

  protected PreparedStatement getPreparedInsert(String insertStmt)  {
    if (appConfig.useAsyncExecute) {
      if (localCassandraCluster == null) {
        localCassandraCluster = createClusterBuilder(configuration.getContactPoints()).build();
        localCassandraSession = localCassandraCluster.connect();
        // Will create keyspace with "if not exists" option.
        createKeyspace(localCassandraSession, keyspace);
      }
      if (localPreparedInsert == null) {
        localPreparedInsert = localCassandraSession.prepare(insertStmt);
      }
      return localPreparedInsert;
    } else {
      if (preparedInsert == null) {
        synchronized (prepareInitLock) {
          if (preparedInsert == null) {
            // Create the prepared statement object.
            preparedInsert = getCassandraClient().prepare(insertStmt);
          }
        }
      }
      return preparedInsert;
    }
  }

  protected abstract BoundStatement bindInsert(String key, ByteBuffer value);

  @Override
  public long doWrite(int threadIdx) {
    SimpleLoadGenerator.Key key = getSimpleLoadGenerator().getKeyToWrite();
    if (key == null) {
      return 0;
    }

    int write_count = 0;
    if (appConfig.useAsyncExecute) {
      if (outstandingAsyncWriteRequests.size() >= appConfig.maxAsyncQueueSize) {
        // LOG.info("Got here: " + outstandingAsyncRequests.size());
        for (Map.Entry<SimpleLoadGenerator.Key, ResultSetFuture> entry :
                outstandingAsyncWriteRequests.entrySet()) {
          try {
            ResultSet rs = entry.getValue().get();
            LOG.debug("Wrote key: " + key.toString() + ", return code: " + rs.toString());
            getSimpleLoadGenerator().recordWriteSuccess(entry.getKey());
            write_count++;
          } catch (Exception e) {
            getSimpleLoadGenerator().recordWriteFailure(entry.getKey());
            LOG.info("Got exception: " + e.getMessage());
          }
        }
        outstandingAsyncWriteRequests.clear();
      }
    }

    try {
      // Do the write to Cassandra.
      BoundStatement insert;
      if (appConfig.valueSize == 0) {
        String value = key.getValueStr();
        insert = bindInsert(key.asString(), ByteBuffer.wrap(value.getBytes()));
      } else {
        byte[] value = getRandomValue(key);
        insert = bindInsert(key.asString(), ByteBuffer.wrap(value));
      }

      if (appConfig.useAsyncExecute) {
        outstandingAsyncWriteRequests.put(key, localCassandraSession.executeAsync(insert));
        return write_count;
      } else {
        ResultSet resultSet = getCassandraClient().execute(insert);
        LOG.debug("Wrote key: " + key.toString() + ", return code: " + resultSet.toString());
        getSimpleLoadGenerator().recordWriteSuccess(key);
        return 1;
      }
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      throw e;
    }
  }

  @Override
  public void appendMessage(StringBuilder sb) {
    super.appendMessage(sb);
    sb.append("maxWrittenKey: " + getSimpleLoadGenerator().getMaxWrittenKey() +  " | ");
    sb.append("maxGeneratedKey: " + getSimpleLoadGenerator().getMaxGeneratedKey() +  " | ");
  }

  public void appendParentMessage(StringBuilder sb) {
    super.appendMessage(sb);
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on Cassandra. The app writes out 1M unique string keys",
        "each with a string value. There are multiple readers and writers that update these",
        "keys and read them indefinitely. Note that the number of reads and writes to",
        "perform can be specified as a parameter.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--value_size " + appConfig.valueSize,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--table_ttl_seconds " + appConfig.tableTTLSeconds);
  }
}
