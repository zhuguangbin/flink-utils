package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.util.URLParser;

import java.util.concurrent.TimeUnit;

public abstract class AbstractExternalPersister implements ExternalPersistExecutor {
    public static final int BATCH_THRESHOLD = 200;
    public static final int RETRY_BACKOFF_MILLIS = 100;
    public static final int RETRY_BACKOFF_MAX = 3;
    private IAerospikeClient client;
    private WritePolicy writePolicy;

    public AbstractExternalPersister(WritePolicy wp, IAerospikeClient client) {
        this.client = client;
        writePolicy = new WritePolicy(wp);
        writePolicy.replica = Replica.MASTER_PROLES;
        writePolicy.commitLevel = CommitLevel.COMMIT_MASTER;
        writePolicy.sendKey = true;
    }

    public void put(Key key, Bin[] bins) throws InterruptedException {
        int i = 0;
        while (true) {
            try {
                client.put(writePolicy, key, bins);
                return;
            } catch (AerospikeException e) {
                if (i++ >= RETRY_BACKOFF_MAX) {
                    throw e;
                }
                TimeUnit.MILLISECONDS.sleep(RETRY_BACKOFF_MILLIS);
            }
        }
    }

    public void delete(Key key) throws InterruptedException {
        int i = 0;
        while (true) {
            try {
                client.delete(writePolicy, key);
                return;
            } catch (AerospikeException e) {
                if (i++ >= RETRY_BACKOFF_MAX) {
                    throw e;
                }
                TimeUnit.MILLISECONDS.sleep(RETRY_BACKOFF_MILLIS);
            }
        }
    }

    public IAerospikeClient getClient() {
        return client;
    }

    public WritePolicy getWritePolicy() {
        return writePolicy;
    }
}
