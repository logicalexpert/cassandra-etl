package scriptella.driver.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import scriptella.configuration.QueryEl;
import scriptella.configuration.ScriptEl;
import scriptella.configuration.ScriptingElement;
import scriptella.core.DynamicContext;
import scriptella.core.DynamicContextDecorator;
import scriptella.core.EtlCancelledException;
import scriptella.core.EtlVariable;
import scriptella.core.ExecutableElement;
import scriptella.core.QueryExecutor;
import scriptella.core.ScriptExecutor;
import scriptella.jdbc.JdbcException;
import scriptella.jdbc.SqlParserBase;
import scriptella.jdbc.SqlReaderTokenizer;
import scriptella.jdbc.SqlTokenizer;
import scriptella.spi.AbstractConnection;
import scriptella.spi.ParametersCallback;
import scriptella.spi.QueryCallback;
import scriptella.spi.Resource;

import com.datastax.driver.core.PreparedStatement;


class CassandraQueryExecutor extends SqlParserBase implements Closeable {
    private static final Logger LOG = Logger.getLogger(CassandraQueryExecutor.class.getName());
    protected final Resource resource;
    protected final CassandraConnection connection;
    protected final CassandraStatementCache cache;
    private QueryCallback callback;
    private ParametersCallback paramsCallback;
    private List<Object> params = new ArrayList<Object>();
    private int updateCount;//number of updated rows
    private final AbstractConnection.StatementCounter counter = null;
    private SqlTokenizer cachedTokenizer;
    
    public CassandraQueryExecutor(final Resource resource, final CassandraConnection connection) {
        this.resource = resource;
        this.connection = connection;
        cache = new CassandraStatementCache(connection.getCassandraSession(), 10, 0, 10000);
//        counter = connection.getStatementCounter();
    }

    final void execute(final ParametersCallback parametersCallback) {
        execute(parametersCallback, null);
    }

    final void execute(final ParametersCallback parametersCallback, final QueryCallback queryCallback) {
        paramsCallback = parametersCallback;
        callback = queryCallback;
        updateCount = 0;
        SqlTokenizer tok = cachedTokenizer;
        boolean cache = false;
        if (tok==null) { //If not cached
            try {
                final Reader reader = resource.open();
                tok = new SqlReaderTokenizer(reader, connection.separator,
                        connection.separatorSingleLine, connection.keepformat);
                cache = reader instanceof StringReader;
            } catch (IOException e) {
                throw new JdbcException("Failed to open resource", e);
            }
        }
        parse(tok);
        //We should remember cached tokenizer only if all statements were parsed
        //i.e. no errors occured
        if (cache) {
            cachedTokenizer=tok;
        }
    }

    int getUpdateCount() {
        return updateCount;
    }


    @Override
    protected String handleParameter(final String name,
                                     final boolean expression, boolean jdbcParam) {
        Object p;

        if (expression) {
            p = connection.getParametersParser().evaluate(name, paramsCallback);
        } else {
            p = paramsCallback.getParameter(name);
        }

        if (jdbcParam) { //if insert as prepared stmt parameter
            params.add(p);
            return "?";
        } else { //otherwise return string representation.
            //todo we need to defines rules for toString transformations
            return p == null ? super.handleParameter(name, expression, jdbcParam) : p.toString();
        }
    }

    @Override
    public void statementParsed(final String sql) {
        EtlCancelledException.checkEtlCancelled();
        CassandraStatementWrapper<PreparedStatement> sw = null;
        try {
            sw = cache.prepare(sql, params);
            int updatedRows = -1;
            if (callback != null) {
                sw.query(connection.getCassandraSession(), callback, paramsCallback);
            } else {
                updatedRows = sw.update(connection.getCassandraSession());
            }
            if (connection.autocommitSize > 0 && (counter.statements % connection.autocommitSize == 0)) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Committing transaction after " + connection.autocommitSize + " statements");
                }
                connection.commit();
            }
        } catch (Exception e) {
            throw new CassandraProviderException(e);
        } finally {
            params.clear();
        }

    }

    public void close() {
    }

    private final class QueryCtxDecorator extends DynamicContextDecorator implements QueryCallback {
        private ParametersCallback params;
        private int rownum; //current row number
        private final Map<String, Object> cachedParams = new HashMap<String, Object>();

        public QueryCtxDecorator(DynamicContext context) {
            super(context);
        }

        public void processRow(final ParametersCallback parameters) {
            EtlCancelledException.checkEtlCancelled();
            rownum++;
            params = parameters;
            cachedParams.clear();
            execute(this);
        }

        @Override
        public final Object getParameter(final String name) {
            if ("rownum".equals(name)) { //return current row number
                return rownum;
            }
            if (EtlVariable.NAME.equals(name)) {
                if (etlVariable == null) {
                    etlVariable = new EtlVariable(this, globalContext.getGlobalVariables());
                }
                return etlVariable;
            }

            Object res = cachedParams.get(name);
            if (res == null) {
                res = params.getParameter(name);
                if (isCacheable(res)) {
                    cachedParams.put(name, res);
                }
            }
            return res == null ? null : res;
        }

        /**
         * Check if object is cacheable, i.e. no need to fetch it again.
         *
         * @param o object to check.
         * @return true if object is cacheable.
         */
        private boolean isCacheable(Object o) {
            return !(o instanceof InputStream || o instanceof Reader);
        }

    }

}
