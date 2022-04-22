package io.trino.verifier.queries;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

public class SchemasModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(TrinoClient.class).in(Scopes.SINGLETON);
        buildConfigObject(SchemasConfig.class);
    }
}
