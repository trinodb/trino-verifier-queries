package io.trino.verifier.queries;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class SchemasConfig
{
    private String trinoUrl;
    private String trinoUsername = "verifier";
    private String trinoPassword;
    private boolean forceWritePartitioning = true;

    @NotNull
    public String getTrinoUrl()
    {
        return trinoUrl;
    }

    @ConfigDescription("Trino instance to be used to generate schemas")
    @Config("trino-url")
    public SchemasConfig setTrinoUrl(String trinoUrl)
    {
        this.trinoUrl = trinoUrl;
        return this;
    }

    @NotNull
    public String getTrinoUsername()
    {
        return trinoUsername;
    }

    @Config("trino-username")
    public SchemasConfig setTrinoUsername(String trinoUsername)
    {
        this.trinoUsername = trinoUsername;
        return this;
    }

    public Optional<String> getTrinoPassword()
    {
        return Optional.ofNullable(trinoPassword);
    }

    @Config("trino-password")
    public SchemasConfig setTrinoPassword(String trinoPassword)
    {
        this.trinoPassword = trinoPassword;
        return this;
    }

    public boolean isForceWritePartitioning()
    {
        return forceWritePartitioning;
    }

    @Config("force-write-partitioning")
    @ConfigDescription("Force write partitioning when creating partitioned tables")
    public SchemasConfig setForceWritePartitioning(boolean forceWritePartitioning)
    {
        this.forceWritePartitioning = forceWritePartitioning;
        return this;
    }
}
