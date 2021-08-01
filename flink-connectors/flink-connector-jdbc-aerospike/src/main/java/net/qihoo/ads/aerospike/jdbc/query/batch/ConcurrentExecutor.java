package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.WritePolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ConcurrentExecutor extends AbstractExternalPersister {
    private static final Logger logger = Logger.getLogger(ConcurrentExecutor.class.getName());
    private static final int REJECT_WAITING_MILLIS = 100;
    private static final int REJECT_WAITING_RETRY = 3;
    private ExecutorService executor;
    private final ThreadPoolQueueMonitor queueMonitor;
    public ConcurrentExecutor(WritePolicy wp, IAerospikeClient client, ExecutorService executor) {
        super(wp, client);
        this.executor = executor;
        this.queueMonitor = new ThreadPoolQueueMonitor((ThreadPoolExecutor) executor, 1000);
    }

    @Override
    public int[] execute(List<BatchEntity> entities) {
        long st = System.currentTimeMillis();
        List<Future<Integer>> fs = new ArrayList<>();
        int total = 0;
        try {
            for (int start = 0, end; start < entities.size(); start = end) {
                end = Math.min(start + BATCH_THRESHOLD, entities.size());
                final List<BatchEntity> toPersist = entities.subList(start, end);
                int i = 0;
                while (true) {
                    try {
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
                        break;
                    } catch (RejectedExecutionException e) {
                        if (i++ > REJECT_WAITING_RETRY) {
                            throw e;
                        }
                        TimeUnit.MILLISECONDS.sleep(REJECT_WAITING_MILLIS);
                    }
                }
            }
            for (Future<Integer> f : fs) {
                total += f.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            queueMonitor.trigger();
        }
        logger.info(String.format("concurrent execute size: %d, batch: %d, cost: %d ms", entities.size(), BATCH_THRESHOLD, System.currentTimeMillis() - st));
        return new int[] {total};
    }

    static class ThreadPoolQueueMonitor {
        final ThreadPoolExecutor executor;
        int lastSize;
        final int monitorDelta;

        public ThreadPoolQueueMonitor(ThreadPoolExecutor executor, int monitorDelta) {
            this.executor = executor;
            this.lastSize = 0;
            this.monitorDelta = monitorDelta;
        }

        public synchronized void trigger() {
           int currSize = executor.getQueue().size();
           if (currSize / monitorDelta != lastSize / monitorDelta) {
               logger.info(String.format("now concurrent queue size is %d, while last value is %d", currSize, lastSize));
           }
           lastSize = currSize;
        }
    }

}
