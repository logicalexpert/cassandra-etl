package scriptella.driver.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import scriptella.util.LRUMap;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.mysql.jdbc.jdbc2.optional.StatementWrapper;

class CassandraStatementCache implements Closeable {
    private Map<String, CassandraStatementWrapper> map;
    private final Session session;
    private int batchSize;
    private int fetchSize;

    /**
     * Creates a statement cache for specified session.
     *
     * @param session session to create cache for.
     * @param size       cache size, 0 or negative means disable cache.
     * @param batchSize  size of prepared statements batch.
     * @param fetchSize  see {@link java.sql.Statement#setFetchSize(int)}
     */
    public CassandraStatementCache(Session session, final int size, final int batchSize, final int fetchSize) {
        this.session = session;
        this.batchSize = batchSize;
        this.fetchSize = fetchSize;
        if (size > 0) { //if cache is enabled
            map = new CacheMap(size);
        }
    }

    /**
     * Prepares a statement.
     * <p>The sql is used as a key to lookup a {@link StatementWrapper},
     * if cache miss the statement is created and put to cache.
     *
     * @param sql    statement SQL.
     * @param params parameters for SQL.
     * @return a wrapper for specified SQL.
     * @throws SQLException if DB reports an error
     * @see StatementWrapper
     */
    public CassandraStatementWrapper prepare(final String sql, final List<Object> params) throws SQLException {
        //In batch mode always use Batched statement for sql without parameters
    	CassandraStatementWrapper sw = map == null ? null : map.get(sql);

        if (sw == null) { //If not cached
                sw = prepare(sql);
                put(sql, sw);
		        sw.setParameters(params);
        }
        return sw;
    }

    /**
     * Testable template method to create prepared statement
     */
    protected CassandraStatementWrapper prepare(final String sql) throws SQLException {
    	CassandraStatementWrapper preparedStatement = new CassandraStatementWrapper<PreparedStatement>(session.prepare(sql), new CassandraTypesConverter());
        return preparedStatement;
    }

    private boolean isBatchMode() {
        return batchSize > 0;
    }

    private void put(String key, CassandraStatementWrapper entry) {
        if (map != null) {
            map.put(key, entry);
        }
    }

    /**
     * LRU Map implementation for statement cache.
     */
    private static class CacheMap extends LRUMap<String, CassandraStatementWrapper> {
        private static final long serialVersionUID = 1;

        public CacheMap(int size) {
            super(size);
        }

        protected void onEldestEntryRemove(Map.Entry<String, CassandraStatementWrapper> eldest) {
        }
    }

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
