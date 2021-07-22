package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.IAerospikeClient;

import java.util.List;
import java.util.logging.Logger;

public class SequentialExecutor extends AbstractExternalPersister {
    private static final Logger logger = Logger.getLogger(SequentialExecutor.class.getName());
    public SequentialExecutor(IAerospikeClient client) {
        super(client);
    }

    @Override
    public int[] execute(List<BatchEntity> entities) {
        int cnt = 0;
        long st = System.currentTimeMillis();
        for(BatchEntity entity : entities) {
            if (entity.type.equals(BatchType.DELETE)) {
                delete(entity.key);
            } else put(entity.key, entity.bins);
            ++ cnt;
        }
        logger.info(String.format("sequence execute size: %d, cost: %d ms", entities.size(), System.currentTimeMillis() - st));
        return new int[]{cnt};
    }

}
