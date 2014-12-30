package scriptella.driver.cassandra;

import java.io.Closeable;
import java.sql.SQLException;

import scriptella.jdbc.JdbcException;
import scriptella.jdbc.JdbcUtils;
import scriptella.spi.ParametersCallback;
import scriptella.util.ColumnsMap;
import scriptella.util.IOUtils;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;


/**
 * Represents SQL query result set as {@link ParametersCallback}.
 * <p>This class exposes pseudo column <code>rownum</code> -current row number starting at 1.
 *
 * @author Fyodor Kupolov
 * @version 1.0
 */
public class ResultSetAdapter implements ParametersCallback, Closeable {

    private ResultSet resultSet;
    private ColumnsMap columnsMap;
    private ParametersCallback params; //parent parameters callback to use
    private CassandraTypesConverter converter;
    private int columnsCount;
    private String[] jdbcTypes;
    private Row row;

    /**
     * Instantiates an adapter, prepares a cache and builds a map of column names.
     *
     * @param resultSet          resultset to adapt.
     * @param parametersCallback parent parameter callback.
     * @param converter          type converter to use for getting column values as object.
     */
    public ResultSetAdapter(ResultSet resultSet,
                            ParametersCallback parametersCallback, CassandraTypesConverter converter) {
        this.params = parametersCallback;
        this.resultSet = resultSet;
        this.converter = converter;
    }

    private void initMetaData() {
        columnsMap = new ColumnsMap();
        try {
            ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
            columnsCount = columnDefinitions.size();
            jdbcTypes = new String[columnsCount];
            for (int i = 1; i <= columnsCount; i++) {
                columnsMap.registerColumn(columnDefinitions.getName(i), i);
                columnsMap.registerColumn(columnDefinitions.getName(i), i);
                jdbcTypes[i-1]=columnDefinitions.getType(i).getName().name(); //Store column types for converter
            }
        } catch (Exception e) {
            throw new CassandraProviderException("Unable to process result set ", e);
        }
    }

    /**
     * @return true if the new current row is valid; false if there are no more rows
     * @see java.sql.ResultSet#next()
     */
    public boolean next() {
        try {
            if(!resultSet.isFullyFetched()) {
            	converter.close();
            	row = resultSet.one();
            	return true;
            }
            return false;
        } catch (Exception e) {
            throw new CassandraProviderException("Unable to move cursor to the next row", e);
        }
    }


    public Object getParameter(final String name) {
        if (columnsMap==null) { //if first time access
            initMetaData();
        }
        try {
            Integer index = columnsMap.find(name);
            int ind = index==null?-1:index - 1;
            if (ind >= 0 && ind < columnsCount) { //if index found and in range
                return converter.getObject(row, ind + 1, jdbcTypes[ind]);
            } else { //otherwise call uppper level params
                return params.getParameter(name);
            }
        } catch (SQLException e) {
            throw new JdbcException("Unable to get parameter " + name, e);
        }
    }

    /**
     * Closes the underlying resultset.
     * <p>This method should operate without raising exceptions.
     */
    public void close() {
        if (resultSet != null) {
//            JdbcUtils.closeSilent(resultSet);
            IOUtils.closeSilently(converter);
            resultSet = null;
            params = null;
            columnsMap = null;
            jdbcTypes=null;
        }
    }
}
