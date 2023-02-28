package io.trino.verifier.queries;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.sql.DriverManager;

import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class SuitesModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, String.class);
        binder.bind(Queries.class).in(Scopes.SINGLETON);
        binder.bind(Suites.class).in(Scopes.SINGLETON);
        SuitesConfig config = buildConfigObject(SuitesConfig.class);
        VerifierQueryDao dao = Jdbi.create(() -> DriverManager.getConnection(config.getVerifierDatabase()))
                .installPlugin(new SqlObjectPlugin())
                .onDemand(VerifierQueryDao.class);
        binder.bind(VerifierQueryDao.class).toInstance(dao);
    }
}
