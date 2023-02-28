package io.trino.verifier.queries;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

public interface VerifierQueryDao
{
    @SqlBatch("" +
            "INSERT INTO verifier_queries " +
            "( " +
            "suite, " +
            "name, " +
            "test_catalog, " +
            "test_schema, " +
            "test_query, " +
            "test_session_properties_json, " +
            "control_catalog, " +
            "control_schema, " +
            "control_query, " +
            "control_session_properties_json " +
            ") " +
            "VALUES " +
            "( " +
            ":suite, " +
            ":name, " +
            ":testCatalog, " +
            ":testSchema, " +
            ":testQuery, " +
            ":testSessionPropertiesJson, " +
            ":controlCatalog, " +
            ":controlSchema, " +
            ":controlQuery, " +
            ":controlSessionPropertiesJson " +
            ")")
    void insert(@BindBean List<VerifierQuery> queries);

    @SqlQuery("" +
            "SELECT count(*) " +
            "FROM verifier_queries " +
            "WHERE suite = :suite")
    long getCount(@Bind("suite") String suite);

    @SqlUpdate("DELETE FROM verifier_queries WHERE suite = :suite")
    void delete(@Bind("suite") String suite);
}
