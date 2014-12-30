package scriptella.driver.cassandra;

import scriptella.spi.ProviderException;

public class CassandraProviderException extends ProviderException {
    public CassandraProviderException() {
    }

    public CassandraProviderException(String message) {
        super(message);
    }

    public CassandraProviderException(String message, Throwable cause) {
        super(message, cause);
    }

    public CassandraProviderException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getProviderName() {
        return Driver.DIALECT_IDENTIFIER.getName();
    }

    @Override
    public ProviderException setErrorStatement(String errStmt) {
        return super.setErrorStatement(errStmt);
    }

}
