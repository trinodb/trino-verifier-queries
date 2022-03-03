package io.starburst.verifier;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class SuitesConfig
{
    private String verifierDatabase;

    @NotNull
    public String getVerifierDatabase()
    {
        return verifierDatabase;
    }

    @ConfigDescription("Database to store query suites to")
    @Config("verifier-database")
    public SuitesConfig setVerifierDatabase(String verifierDatabase)
    {
        this.verifierDatabase = verifierDatabase;
        return this;
    }
}
