package com.aerospike.jdbc;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.query.AerospikeQueryParser;
import com.aerospike.jdbc.query.QueryPerformer;
import net.qihoo.ads.aerospike.jdbc.query.batch.BatchQueryHandler;
import com.aerospike.jdbc.sql.type.ByteArrayBlob;
import com.aerospike.jdbc.sql.type.StringClob;
import com.aerospike.jdbc.util.AuxStatementParser;
import com.aerospike.jdbc.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Optional;

import static com.aerospike.jdbc.util.PreparedStatement.parseParameters;
import static java.lang.String.format;

public class AerospikePreparedStatement extends AerospikeStatement implements PreparedStatement {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeConnection.class);

    private Object[] parameterValues;

    public AerospikePreparedStatement(IAerospikeClient client, Connection connection, String sql) throws SQLException {
        super(client, connection);
        this.sql = sql;
        int n = parseParameters(sql, 0).getValue();
        parameterValues = new Object[n];
        Arrays.fill(parameterValues, Optional.empty());
        query = parseQuery(sql, connection);
        query.setStatement(this);
    }

    private AerospikeQuery parseQuery(String sql, Connection connection) throws SQLException {
        final String sqlEscape = sql.replaceAll("\n", " ");
        AerospikeQuery query = AuxStatementParser.hack(sqlEscape).orElseGet(() -> {
            io.trino.sql.tree.Statement statement = SQL_PARSER.createStatement(sqlEscape, parsingOptions);
            return AerospikeQueryParser.parseSql(statement);
        });
        if (query.getSchema() == null) {
            query.setSchema(connection.getSchema());
        }
        return query;
    }

    @Override
    public ResultSet executeQuery() {
        logger.debug("AerospikePreparedStatement executeQuery");
        if (parameterValues != null && parameterValues.length > 0) {
            this.query.addNewQueryBinding(this.parameterValues);
            clearParameters();
        }
        Pair<ResultSet, Integer> result = QueryPerformer.executeQuery(client, this, this.query);
        this.query.resetQueryBindings();
        resultSet = result.getLeft();
        updateCount = result.getRight();
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        Object value = x;
        if (!Value.UseBoolBin) {
            value = x ? 1 : 0;
        }
        setObject(parameterIndex, value);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, "\"" + x + "\"");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream() is deprecated");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBlob(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() {
        Arrays.fill(parameterValues, Optional.empty());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (parameterIndex <= 0 || parameterIndex > parameterValues.length) {
            throw new SQLException(parameterValues.length == 0 ?
                    "Current SQL statement does not have parameters" :
                    format("Wrong parameter index. Expected from %d till %d", 1, parameterValues.length));
        }
        parameterValues[parameterIndex - 1] = x;
    }

    @Override
    public boolean execute() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private String prepareQuery() {
        return String.format(this.sql.replace("?", "%s"), parameterValues);
    }

    @Override
    public void addBatch() {
        logger.debug("AerospikePreparedStatement addBatch: {}", Arrays.toString(this.parameterValues));
        this.query.addNewQueryBinding(this.parameterValues);
        clearParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        logger.debug("AerospikePreparedStatement executeBatch");
        BatchQueryHandler queryHandler = new BatchQueryHandler(((AerospikeConnection)getConnection()).up.getWritePolicy(), client, this);
        int[] rst = queryHandler.executeBatch(query);
        query.resetQueryBindings();
        return rst;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob blob) throws SQLException {
        setBytes(parameterIndex, blob.getBytes(1, (int) blob.length()));
    }

    @Override
    public void setClob(int parameterIndex, Clob clob) throws SQLException {
        setString(parameterIndex, clob.getSubString(1, (int) clob.length()));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException {
        setString(parameterIndex, url != null ? url.toString() : null);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob clob) throws SQLException {
        setString(parameterIndex, clob.getSubString(1, (int) clob.length()));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            String result = IOUtils.toString(reader);
            if (result.length() != length) {
                throw new SQLException(format("Unexpected data length: expected %s but was %d", length, result.length()));
            }
            setObject(parameterIndex, result);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        byte[] bytes = new byte[(int) length];
        DataInputStream dis = new DataInputStream(inputStream);
        try {
            dis.readFully(bytes);
            if (inputStream.read() != -1) {
                throw new SQLException(format("Source contains more bytes than required %d", length));
            }
            setBytes(parameterIndex, bytes);
        } catch (EOFException e) {
            throw new SQLException(format("Source contains less bytes than required %d", length), e);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setClob(parameterIndex, new InputStreamReader(x), length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBlob(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setClob(parameterIndex, new InputStreamReader(x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBlob(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            setClob(parameterIndex, new StringClob(IOUtils.toString(reader)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            setBlob(parameterIndex, new ByteArrayBlob(IOUtils.toByteArray(inputStream)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

}
