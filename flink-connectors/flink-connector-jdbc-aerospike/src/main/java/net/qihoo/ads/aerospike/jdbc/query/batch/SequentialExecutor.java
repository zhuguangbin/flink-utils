package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.WritePolicy;

import java.util.List;
import java.util.logging.Logger;

public class SequentialExecutor extends AbstractExternalPersister {
    private static final Logger logger = Logger.getLogger(SequentialExecutor.class.getName());
    public SequentialExecutor(WritePolicy wp, IAerospikeClient client) {
        super(wp, client);
    }

    @Override
    public int[] execute(List<BatchEntity> entities) {
        int cnt = 0;
        long st = System.currentTimeMillis();
        for(BatchEntity entity : entities) {
            try {
                if (entity.type.equals(BatchType.DELETE)) {
                    delete(entity.key);
                } else put(entity.key, entity.bins);
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            ++ cnt;
        }
        logger.info(String.format("sequence execute size: %d, cost: %d ms", entities.size(), System.currentTimeMillis() - st));
        return new int[]{cnt};
    }

}
