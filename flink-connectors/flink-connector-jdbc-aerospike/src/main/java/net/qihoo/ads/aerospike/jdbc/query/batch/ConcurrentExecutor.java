package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.IAerospikeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class ConcurrentExecutor extends AbstractExternalPersister {
    private static final Logger logger = Logger.getLogger(ConcurrentExecutor.class.getName());
    private ExecutorService executor;
    public ConcurrentExecutor(IAerospikeClient client, ExecutorService executor) {
        super(client);
        this.executor = executor;
    }

    @Override
    public int[] execute(List<BatchEntity> entities) {
        long st = System.currentTimeMillis();
        List<Future<Integer>> fs = new ArrayList<>();
        for (int start = 0, end; start < entities.size(); start = end) {
            end = Math.min(start + BATCH_THRESHOLD, entities.size());
            final List<BatchEntity> toPersist = entities.subList(start, end);
            fs.add(executor.submit(() -> {
                int cnt = 0;
                for(BatchEntity entity : toPersist) {
                    if (entity.type.equals(BatchType.DELETE)) {
                        delete(entity.key);
                    } else put(entity.key, entity.bins);
                    ++ cnt;
                }
                return cnt;
            }));
        }
        int total = 0;
        try {
            for (Future<Integer> f : fs) {
                total += f.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        logger.info(String.format("concurrent execute size: %d, batch: %d, cost: %d ms", entities.size(), BATCH_THRESHOLD, System.currentTimeMillis() - st));
        return new int[] {total};
    }

}
