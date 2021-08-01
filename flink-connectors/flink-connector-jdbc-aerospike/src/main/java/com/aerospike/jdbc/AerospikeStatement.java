package com.aerospike.jdbc;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.query.AerospikeQueryParser;
import com.aerospike.jdbc.query.QueryPerformer;
import com.aerospike.jdbc.sql.SimpleWrapper;
import com.aerospike.jdbc.util.AuxStatementParser;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.logging.Logger;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.lang.String.format;
import static java.sql.ResultSet.*;

public class AerospikeStatement implements Statement, SimpleWrapper {

    private static final Logger logger = Logger.getLogger(AerospikeStatement.class.getName());

    public static final SqlParser SQL_PARSER = new SqlParser();
    public static final ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE);

    protected final IAerospikeClient client;
    private final Connection connection;
    private int maxRows = Integer.MAX_VALUE;
    private int queryTimeout;
    private ResultSet resultSet;
    private int updateCount;
    private String schema;

    String sql;
    AerospikeQuery query;

    public AerospikeStatement(IAerospikeClient client, Connection connection) {
        this.client = client;
        this.connection = connection;
        try {
            this.schema = connection.getSchema();
        } catch (SQLException e) {
            logger.warning(e.getMessage());
        }
    }

    @Override
    public ResultSet executeQuery(String sql) {
        logger.info("executeQuery: " + sql);
        this.sql = sql;
        this.query = parseQuery(sql);
        this.query.setStatement(this);
        Pair<ResultSet, Integer> result = QueryPerformer.executeQuery(client, this, this.query);
        resultSet = result.getLeft();
        updateCount = result.getRight();

        return resultSet;
    }

    private AerospikeQuery parseQuery(String sql) {
        final String sqlEscape = sql.replaceAll("\n", " ");
        AerospikeQuery query = AuxStatementParser.hack(sqlEscape).orElseGet(() -> {
            io.trino.sql.tree.Statement statement = SQL_PARSER.createStatement(sqlEscape, parsingOptions);
            return AerospikeQueryParser.parseSql(statement);
        });
        if (query.getSchema() == null) {
            query.setSchema(schema);
        }
        return query;
    }

    @Override
    public int executeUpdate(String sql) {
        executeQuery(sql);
        return updateCount;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int getMaxFieldSize() {
        return 8 * 1024 * 1024; // 8 MB
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Max field size cannot be changed dynamically");
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // do nothing
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement cannot be canceled");
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // TODO make use of warnings
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursor is not supported");
    }

    @Override
    public boolean execute(String sql) {
        resultSet = executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public int getUpdateCount() {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLException(format("Attempt to set unsupported fetch direction %d. " +
                    "Only FETCH_FORWARD=%d is supported. The value is ignored.", direction, FETCH_FORWARD));
        }
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
        // do nothing supported size = 1.
    }

    @Override
    public int getFetchSize() {
        return 1;
    }

    @Override
    public int getResultSetConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch update is not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch update is not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch update is not supported");
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLFeatureNotSupportedException("Statement does not support pools");
        }
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() {
        // do nothing
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }
}
