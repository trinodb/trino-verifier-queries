package io.trino.verifier.queries;

import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class Query
{
    private final String id;
    private final String sql;
    private final Map<String, String> sessionProperties;

    public Query(String id, String sql, Map<String, String> sessionProperties)
    {
        this.id = requireNonNull(id, "id is null");
        this.sql = requireNonNull(sql, "sql is null");
        this.sessionProperties = Map.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
    }

    public String getId()
    {
        return id;
    }

    public String getSql()
    {
        return sql;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    public Query preProcessSql(Function<String, String> processor)
    {
        return new Query(
                id,
                processor.apply(sql),
                sessionProperties);
    }
}
