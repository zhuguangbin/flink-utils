package net.qihoo.ads.aerospike.jdbc.query.batch;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.jdbc.AerospikeConnection;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.model.QueryType;
import com.aerospike.jdbc.query.BaseQueryHandler;
import com.aerospike.jdbc.util.IOUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public class BatchQueryHandler extends BaseQueryHandler {

    private ExternalPersistExecutor externalPersistExecutor;

    public BatchQueryHandler(IAerospikeClient client, Statement statement) throws SQLException {
        super(client, statement);
        this.externalPersistExecutor = new DelegateExecutor(client, ((AerospikeConnection)statement.getConnection()).getEs());
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        throw new RuntimeException("use executeBatch(AerospikeQuery) instead");
    }

    public int[] executeBatch(AerospikeQuery query) {
        List<ExternalPersistExecutor.BatchEntity> entities = query.getType().equals(QueryType.DELETE) ? buildDelete(query) : buildUpsert(query);
        return entities.size() > 0 ? externalPersistExecutor.execute(entities) : new int[]{0};
    }

    private List<ExternalPersistExecutor.BatchEntity> buildUpsert(AerospikeQuery query) {
        int keyIndex = primaryIndex(query);
        return query.getQueryBindings().stream().map(p -> {
            Key key = null;
            Bin[] bins = new Bin[p.length - 1];
            for (int i = 0;i < p.length;++i) {
                if (i == keyIndex) {
                    key = new Key(query.getSchema(), query.getTable(), getBinValue(p[i].toString()));
                } else{
                    int binIndex = i < keyIndex ? i : i - 1;
                    bins[binIndex] = new Bin(IOUtils.stripQuotes(query.getColumns().get(i)).trim(), getBinValue(p[i].toString()));
                }
            }
            return new ExternalPersistExecutor.BatchEntity(key, bins);
        }).collect(Collectors.toList());
    }

    private List<ExternalPersistExecutor.BatchEntity> buildDelete(AerospikeQuery query) {
        return query.getQueryBindings().stream().map(o -> {
            Key key = new Key(query.getSchema(), query.getTable(), getBinValue(o[0].toString()));
            return new ExternalPersistExecutor.BatchEntity(key);
        }).collect(Collectors.toList());
    }

    protected final int primaryIndex(AerospikeQuery query) {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(defaultKeyName)) {
                return i;
            }
        }
        throw new RuntimeException("primary key must specified in batch mode");
    }


}
