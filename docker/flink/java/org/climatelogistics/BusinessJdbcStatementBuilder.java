package org.climatelogistics;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.types.Row;

/** Binds the business UPSERT's JSON payload and nullable alert identity. */
public final class BusinessJdbcStatementBuilder implements JdbcStatementBuilder<Row> {
    /** Validate the Flink row shape and expose its two SQL parameter values. */
    public String[] getParameters(Row row) {
        if (row == null || row.getArity() != 2) {
            throw new IllegalArgumentException("business JDBC row must contain exactly two fields");
        }
        Object payload = row.getField(0);
        Object alertId = row.getField(1);
        if (!(payload instanceof String)) {
            throw new IllegalArgumentException("business JDBC payload must be a non-null string");
        }
        if (alertId != null && !(alertId instanceof String)) {
            throw new IllegalArgumentException("business JDBC alert id must be a string or null");
        }
        return new String[] {(String) payload, (String) alertId};
    }

    @Override
    public void accept(PreparedStatement statement, Row row) throws SQLException {
        String[] parameters = getParameters(row);
        statement.setString(1, parameters[0]);
        if (parameters[1] == null) {
            statement.setNull(2, Types.VARCHAR);
        } else {
            statement.setString(2, parameters[1]);
        }
    }
}
