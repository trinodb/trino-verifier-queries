package io.starburst.verifier;

import static java.util.Objects.requireNonNull;

public class Query
{
    private final String id;
    private final String sql;

    public Query(String id, String sql)
    {
        this.id = requireNonNull(id, "id is null");
        this.sql = requireNonNull(sql, "sql is null");
    }

    public String getId()
    {
        return id;
    }

    public String getSql()
    {
        return sql;
    }
}
