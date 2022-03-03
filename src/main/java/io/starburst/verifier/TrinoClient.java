package io.starburst.verifier;

import com.google.common.collect.ImmutableList;
import io.trino.jdbc.TrinoConnection;
import io.trino.spi.type.SqlVarbinary;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class TrinoClient
{
    private final String url;
    private final String username;
    private final Optional<String> password;

    private final Set<Connection> activeConnections = synchronizedSet(newIdentityHashSet());

    @Inject
    public TrinoClient(SchemasConfig config)
    {
        this(requireNonNull(config, "config is null").getTrinoUrl(), config.getTrinoUsername(), config.getTrinoPassword());
    }

    public TrinoClient(String url, String username, Optional<String> password)
    {
        this.url = requireNonNull(url, "url is null");
        this.username = requireNonNull(username, "username is null");
        this.password = requireNonNull(password, "password is null");
    }

    public List<String> selectSingleStringColumn(String sql)
    {
        return selectSingleStringColumn(sql, Map.of());
    }

    public List<String> selectSingleStringColumn(String sql, Map<String, String> sessionProperties)
    {
        List<List<Object>> rows = execute(sql, sessionProperties);
        ImmutableList.Builder<String> result = ImmutableList.builder();
        for (List<Object> row : rows) {
            checkArgument(row.size() == 1, "Query expected to return single column, returned %s", row.size());
            Object value = row.get(0);
            if (!(value instanceof String)) {
                throw new IllegalArgumentException(format("Query expected to return single string column, got %s", value.getClass().getName()));
            }
            result.add((String) value);
        }
        return result.build();
    }

    public long executeUpdate(String sql)
    {
        return executeUpdate(sql, Map.of());
    }

    public long executeUpdate(String sql, Map<String, String> sessionProperties)
    {
        List<List<Object>> rows = execute(sql, sessionProperties);
        checkArgument(rows.size() == 1, "Query expected to return a single row, returned %s", rows.size());
        List<Object> values = rows.get(0);
        checkArgument(values.size() == 1, "Query expected to return single value, returned %s", values.size());
        checkArgument(values.get(0) instanceof Long, "Value is expected to be of type of Long, got %s", values.get(0).getClass().getName());
        return (Long) values.get(0);
    }

    public List<List<Object>> execute(String sql)
    {
        return execute(sql, Map.of());
    }

    public List<List<Object>> execute(String sql, Map<String, String> sessionProperties)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password.orElse(null))) {
            sessionProperties.forEach(connection.unwrap(TrinoConnection.class)::setSessionProperty);
            try {
                activeConnections.add(connection);
                try (Statement statement = connection.createStatement()) {
                    boolean isSelectQuery = statement.execute(sql);
                    if (isSelectQuery) {
                        return convertJdbcResultSet(statement.getResultSet());
                    }
                    else {
                        return List.of(List.of(statement.getLargeUpdateCount()));
                    }
                }
            }
            finally {
                activeConnections.remove(connection);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failure running trino query", e);
        }
    }

    public void closeActiveConnections()
    {
        synchronized (activeConnections) {
            for (Connection connection : activeConnections) {
                try {
                    connection.close();
                }
                catch (SQLException e) {
                    // ignore
                }
            }
            activeConnections.clear();
        }
    }

    // https://github.com/trinodb/trino/blob/a2fafca104c4cae50998ad6d8a52e2f322712c62/service/trino-verifier/src/main/java/io/trino/verifier/Validator.java#L571
    private List<List<Object>> convertJdbcResultSet(ResultSet resultSet)
            throws SQLException
    {
        int columnCount = resultSet.getMetaData().getColumnCount();

        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object object = resultSet.getObject(i);
                if (object instanceof BigDecimal) {
                    if (((BigDecimal) object).scale() <= 0) {
                        object = ((BigDecimal) object).longValueExact();
                    }
                    else {
                        object = ((BigDecimal) object).doubleValue();
                    }
                }
                if (object instanceof Array) {
                    object = ((Array) object).getArray();
                }
                if (object instanceof byte[]) {
                    object = new SqlVarbinary((byte[]) object);
                }
                row.add(object);
            }
            rows.add(unmodifiableList(row));
        }
        return rows.build();
    }
}
