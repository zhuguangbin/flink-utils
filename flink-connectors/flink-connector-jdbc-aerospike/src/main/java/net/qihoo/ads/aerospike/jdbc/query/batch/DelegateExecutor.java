package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.IAerospikeClient;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

public class DelegateExecutor extends AbstractExternalPersister {
    private static final Logger logger = Logger.getLogger(DelegateExecutor.class.getName());

    private ExternalPersistExecutor persistExecutor;

    public DelegateExecutor(IAerospikeClient client, ExecutorService executor) {
        super(client);
        this.persistExecutor = new ConcurrentExecutor(client, executor);
    }

    @Override
    public int[] execute(List<BatchEntity> entities) {
        int cnt = entities.size();
        if (cnt < BATCH_THRESHOLD) {
            return new SequentialExecutor(getClient()).execute(entities);
        }
        return persistExecutor.execute(entities);
    }

}
