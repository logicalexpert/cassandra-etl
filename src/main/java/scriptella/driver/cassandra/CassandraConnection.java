package scriptella.driver.cassandra;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scriptella.jdbc.ParametersParser;
import scriptella.spi.AbstractConnection;
import scriptella.spi.ConnectionParameters;
import scriptella.spi.ParametersCallback;
import scriptella.spi.ProviderException;
import scriptella.spi.QueryCallback;
import scriptella.spi.Resource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraConnection extends AbstractConnection {

    private static final String[] EMPTY_ARRAY = new String[] {};
    private static Cluster cassandraCluster;
    private static Session cassandraSession;
    private Map<String,String> params;
    private URL url;
    protected String separator = ";";
    protected boolean separatorSingleLine;
    protected boolean keepformat;
	private ParametersParser parametersParser;
	protected int autocommitSize;

    /**
     * Instantiates a new connection to Lucene Query.
     * @param parameters connection parameters.
     */
    public CassandraConnection(ConnectionParameters parameters) {
        super(Driver.DIALECT_IDENTIFIER, parameters);
        cassandraCluster = getNewCluster(parameters.getStringProperty("nodes"));
        cassandraSession = cassandraCluster.connect(parameters.getStringProperty("keyspace"));
        parametersParser = new ParametersParser(parameters.getContext());
    }

	private Cluster getNewCluster(String cassandraNodes) {
		return Cluster.builder()
				.withoutJMXReporting()
				.withoutMetrics()
				.addContactPoints(cassandraNodes.split(","))
				.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, TimeUnit.MINUTES.toMillis(5)))
				.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
				.build();
	}


    public void executeScript(Resource scriptContent, ParametersCallback parametersCallback) throws ProviderException {
        throw new UnsupportedOperationException("Script execution is not supported yet");
    }

    /**
     * Executes a query specified by its content.
     * <p/>
     *
     * @param queryContent       query content.
     * @param parametersCallback callback to get parameter values.
     * @param queryCallback      callback to call for each result set element produced by this query.
     * @see #executeScript(scriptella.spi.Resource, scriptella.spi.ParametersCallback)
     */
    public synchronized void executeQuery(Resource queryContent, ParametersCallback parametersCallback, QueryCallback queryCallback) throws ProviderException{
    	CassandraQueryExecutor q = new CassandraQueryExecutor(queryContent,this);
        q.execute(parametersCallback, queryCallback);
        if (q.getUpdateCount() < 0) {
        	System.out.println("No rows updated in cassandra.");
        }
    	
//    	try {
//    		queryCallback.processRow(parametersCallback);
//			PreparedStatement ps = cassandraSession.prepare(IOUtils.toString(queryContent.open()));
//			cassandraSession.execute(ps.bind());
//	        try {
//	      CassandraQuery query = new CassandraQuery(cassandraSession, parametersCallback, queryCallback);
//      } finally {
//      }

//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//        CassandraQuery query = null;
//        Reader r;
//        try {
//            r = queryContent.open();
//        } catch (IOException e) {
//            throw new TextProviderException("Cannot open a query for reading", e);
//        }
//        try {
//            query = new CassandraQuery(cassandraSession, parametersCallback, queryCallback);
//            query.execute(r);
//        } finally {
//            IOUtils.closeSilently(query);
//        }
    }

    /**
     * Closes the connection and releases all related resources.
     */
    public void close() throws ProviderException {
    	cassandraSession.close();
    	cassandraCluster.close();
    }

	public ParametersParser getParametersParser() {
		return parametersParser;
	}

	public void setParametersParser(ParametersParser parametersParser) {
		this.parametersParser = parametersParser;
	}

	public static Session getCassandraSession() {
		return cassandraSession;
	}

	public static void setCassandraSession(Session cassandraSession) {
		CassandraConnection.cassandraSession = cassandraSession;
	}
}
