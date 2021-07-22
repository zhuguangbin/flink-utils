package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.util.URLParser;

public abstract class AbstractExternalPersister implements ExternalPersistExecutor {
    public static final int BATCH_THRESHOLD = 200;
    private IAerospikeClient client;
    private WritePolicy writePolicy;

    public AbstractExternalPersister(IAerospikeClient client) {
        this.client = client;
        writePolicy = new WritePolicy(URLParser.getWritePolicy());
        writePolicy.sendKey = true;
    }

    public void put(Key key, Bin[] bins) {
        client.put(writePolicy, key, bins);
    }

    public void delete(Key key) {
        client.delete(writePolicy, key);
    }

    public IAerospikeClient getClient() {
        return client;
    }
}
