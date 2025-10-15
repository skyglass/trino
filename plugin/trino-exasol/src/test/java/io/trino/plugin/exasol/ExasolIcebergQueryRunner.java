package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.exasol.ExasolTpchTables.copyAndIngestTpchData;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExasolIcebergQueryRunner
{
    private static final Logger log = Logger.get(ExasolQueryRunner.class);

    private ExasolIcebergQueryRunner() {}

    public static Builder builder(TestingExasolServer server)
    {
        return new Builder(server)
                .addConnectorProperty("connection-url", server.getJdbcUrl())
                .addConnectorProperty("connection-user", "sys")
                .addConnectorProperty("connection-password", "exasol");
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private final TestingExasolServer server;

        private Builder(TestingExasolServer server)
        {
            super(testSessionBuilder()
                    .setCatalog("exasol")
                    .setSchema("default")
                    .build());
            this.server = server;
        }

        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                // TPCH catalog (for data import)
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                // Exasol catalog
                queryRunner.installPlugin(new ExasolPlugin());
                queryRunner.createCatalog("exasol", "exasol", connectorProperties);

                // Iceberg + Nessie catalog ---
                Map<String, String> icebergProperties = new HashMap<>();
                icebergProperties.put("iceberg.catalog.type", "nessie");
                icebergProperties.put("iceberg.nessie-catalog.uri", "http://127.0.0.1:19120/api/v1");
                icebergProperties.put("iceberg.nessie-catalog.ref", "main");
                icebergProperties.put("iceberg.nessie-catalog.default-warehouse-dir", "s3://iceberg");
                icebergProperties.put("fs.native-s3.enabled", "true");
                icebergProperties.put("s3.endpoint", "http://127.0.0.1:9000");
                icebergProperties.put("s3.region", "us-east-1");
                icebergProperties.put("s3.path-style-access", "true");
                icebergProperties.put("s3.aws-access-key", "admin");
                icebergProperties.put("s3.aws-secret-key", "password");

                queryRunner.installPlugin(new IcebergPlugin());
                queryRunner.createCatalog("iceberg", "iceberg", icebergProperties);
                log.info("Created Iceberg (Nessie) catalog with S3 backend");

                // --- Load TPCH data into Exasol ---
                log.info("Loading data into exasol.default...");
                for (TpchTable<?> table : initialTables) {
                    log.info("Importing %s", table.getTableName());
                    String tpchTableName = table.getTableName();
                    MaterializedResult rows = queryRunner.execute(
                            format("SELECT * FROM tpch.tiny.%s", tpchTableName));
                    copyAndIngestTpchData(rows, server, table.getTableName());
                    log.info("Imported %s rows into Exasol table %s", rows.getRowCount(), table.getTableName());
                }

                log.info("Catalog setup complete. Exasol + Iceberg ready for cross-source queries.");
                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = ExasolIcebergQueryRunner.builder(new TestingExasolServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
