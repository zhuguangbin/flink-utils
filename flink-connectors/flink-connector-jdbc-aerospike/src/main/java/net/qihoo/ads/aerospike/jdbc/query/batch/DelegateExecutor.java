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
        /*int cnt = entities.size();
        if (cnt < BATCH_THRESHOLD) {
            return new SequentialExecutor(getWritePolicy(), getClient()).execute(entities);
        }
        return persistExecutor.execute(entities);*/

        // sequential for performance test
        int total = 0;
        for (int start = 0, end; start < entities.size(); start = end) {
            end = Math.min(start + BATCH_THRESHOLD, entities.size());
            final List<BatchEntity> toPersist = entities.subList(start, end);
            total += new SequentialExecutor(getWritePolicy(), getClient()).execute(toPersist)[0];
        }
        return new int[]{total};
    }

}
