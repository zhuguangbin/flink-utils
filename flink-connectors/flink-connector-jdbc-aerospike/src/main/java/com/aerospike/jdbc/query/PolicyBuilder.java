package com.aerospike.jdbc.query;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.AerospikeConnection;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.util.URLParser;

import java.util.Objects;

public final class PolicyBuilder {

    private PolicyBuilder() {
    }

    public static ScanPolicy buildScanPolicy(AerospikeQuery query) {
        URLParser up = ((AerospikeConnection)query.getStatement().getConnection()).getUp();
        ScanPolicy scanPolicy = new ScanPolicy(up.getScanPolicy());
        scanPolicy.maxRecords = Objects.isNull(query.getLimit()) ? 0 : query.getLimit();
        Exp expression = ExpressionBuilder.buildExp(query.getWhere());
        scanPolicy.filterExp = Objects.isNull(expression) ? null : Exp.build(expression);
        return scanPolicy;
    }

    public static Policy buildGetPolicy(AerospikeQuery query) {
        URLParser up = ((AerospikeConnection)query.getStatement().getConnection()).getUp();
        return new Policy(up.getScanPolicy());
    }

    public static ScanPolicy buildScanNoBinDataPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = buildScanPolicy(query);
        scanPolicy.includeBinData = false;
        return scanPolicy;
    }

    public static WritePolicy buildWritePolicy(AerospikeQuery query) {
        URLParser up = ((AerospikeConnection)query.getStatement().getConnection()).getUp();
        WritePolicy writePolicy = new WritePolicy(up.getWritePolicy());
        writePolicy.sendKey = true;
        return writePolicy;
    }

    public static WritePolicy buildCreateOnlyPolicy(AerospikeQuery query) {
        WritePolicy writePolicy = buildWritePolicy(query);
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        return writePolicy;
    }

    public static WritePolicy buildUpdateOnlyPolicy(AerospikeQuery query) {
        WritePolicy writePolicy = buildWritePolicy(query);
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        return writePolicy;
    }
}
