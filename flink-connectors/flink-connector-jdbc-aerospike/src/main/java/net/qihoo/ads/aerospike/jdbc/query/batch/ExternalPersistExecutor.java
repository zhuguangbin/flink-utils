package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import java.util.List;

public interface ExternalPersistExecutor {
    int[] execute(List<BatchEntity> entities);

    class BatchEntity {
        public Key key;
        public Bin[] bins;
        public BatchType type;

        public BatchEntity(Key key, Bin[] bins) {
            this.key = key;
            this.bins = bins;
            this.type = BatchType.UPSERT;
        }

        public BatchEntity(Key key) {
            this.key = key;
            this.type = BatchType.DELETE;
        }
    }

    enum BatchType {
        UPSERT,
        DELETE
    }
}
