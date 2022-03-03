package io.starburst.verifier;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Suite
{
    private final String id;
    private final List<Query> queries;

    public Suite(String id, List<Query> queries)
    {
        this.id = requireNonNull(id, "id is null");
        this.queries = List.copyOf(requireNonNull(queries, "queries is null"));
    }

    public String getId()
    {
        return id;
    }

    public List<Query> getQueries()
    {
        return queries;
    }
}
