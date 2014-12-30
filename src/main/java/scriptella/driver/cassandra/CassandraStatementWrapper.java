package scriptella.driver.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import scriptella.spi.ParametersCallback;
import scriptella.spi.QueryCallback;
import scriptella.util.IOUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Abstraction for {@link java.sql.Statement} and {@link java.sql.PreparedStatement}.
 *
 * @author Fyodor Kupolov
 * @version 1.0
 */
public class CassandraStatementWrapper<T extends PreparedStatement> implements Closeable {
    private static final Logger LOG = Logger.getLogger(CassandraStatementWrapper.class.getName());
    protected final CassandraTypesConverter converter;
    protected final T statement;
    private BoundStatement bindStatement;

    /**
     * For testing only.
     */
    protected CassandraStatementWrapper() {
        converter = null;
        statement = null;
    }

    protected CassandraStatementWrapper(T statement, CassandraTypesConverter converter) {
        if (statement == null) {
            throw new IllegalArgumentException("statement cannot be null");
        }
        if (converter == null) {
            throw new IllegalArgumentException("converter cannot be null");
        }
        this.statement = statement;
        this.converter = converter;
    }

    public int update(Session session) throws Exception {
        session.execute(bindStatement);
        return 1;
    }


    /**
     * Executes the query and returns the result set.
     *
     * @return result set with query result.
     * @throws SQLException if JDBC driver fails to execute the operation.
     */
    public ResultSet query(Session session) throws Exception {
    	return session.execute(bindStatement==null?statement.bind():bindStatement);
    }

    public void query(final Session session, final QueryCallback queryCallback, final ParametersCallback parametersCallback) throws Exception {
        ResultSetAdapter r = null;
        try {
            r = new ResultSetAdapter(query(session), parametersCallback, converter);
            while (r.next()) {
                queryCallback.processRow(r);
            }
        } finally {
            IOUtils.closeSilently(r);
        }
    }

    public void setParameters(final List<Object> values){
    	bindStatement = statement.bind(values.toArray());
    }

    /**
     * Clears any transient state variables, e.g. statement parameters etc.
     */
    public void clear() {
    }

    /**
     * Flushes any pending operations.
     *
     * @return number of rows updated
     * @throws SQLException if DB error occurs.
     */
    public int flush() throws Exception {
        return 0;
    }

    /**
     * @see java.sql.Statement#toString()
     */
    public String toString() {
        return statement.toString();
    }

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
