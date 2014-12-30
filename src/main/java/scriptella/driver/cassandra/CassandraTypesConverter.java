package scriptella.driver.cassandra;

import java.io.Closeable;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import scriptella.util.IOUtils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

class CassandraTypesConverter implements Closeable {
    //Closable resources are registered here to be disposed when
    //they go out of scope, e.g. next query row, or after executing an SQL statement
    private List<Closeable> resources;

    /**
     * Gets the value of the designated column in the current row of this ResultSet
     * object as an Object in the Java programming language.
     *
     * @param rs
     * @param index    column index.
     * @param jdbcType column {@link java.sql.Types JDBC type}
     * @return
     * @throws SQLException
     * @see ResultSet#getObject(int)
     */
    public Object getObject(final Row rs, final int index, final String jdbcType) throws SQLException {
        Object res = rs.getString(index);
        return res;
    }


    protected void registerResource(Closeable resource) {
        if (resources == null) {
            resources = new ArrayList<Closeable>();
        }
        resources.add(resource);
    }

    /**
     * Closes any resources opened during this object lifecycle.
     */
    public void close() {
        if (resources != null) {
            IOUtils.closeSilently(resources);
            resources = null;
        }
    }


}
