package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.WritePolicy;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class DelegateExecutor extends AbstractExternalPersister {

    private ExternalPersistExecutor persistExecutor;

    public DelegateExecutor(WritePolicy wp, IAerospikeClient client, ExecutorService executor) {
        super(wp, client);
        this.persistExecutor = new ConcurrentExecutor(wp, client, executor);
    }

    @Override
    public int[] execute(List<BatchEntity> entities) {
        // int cnt = entities.size();
        if (/*cnt < BATCH_THRESHOLD*/true) {  // since concurrent thread output perform bad, we currently force using SequentialExecutor
            return new SequentialExecutor(getWritePolicy(), getClient()).execute(entities);
        }
        return persistExecutor.execute(entities);
    }

}
