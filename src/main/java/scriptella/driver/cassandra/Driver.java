package scriptella.driver.cassandra;

import scriptella.spi.AbstractScriptellaDriver;
import scriptella.spi.Connection;
import scriptella.spi.ConnectionParameters;
import scriptella.spi.DialectIdentifier;


public class Driver extends AbstractScriptellaDriver {
    static final DialectIdentifier DIALECT_IDENTIFIER = new DialectIdentifier("Cassandra", "2.1.2");


    public Driver() {
    }

    public Connection connect(ConnectionParameters connectionParameters) {
        return new CassandraConnection(connectionParameters);
    }

}
