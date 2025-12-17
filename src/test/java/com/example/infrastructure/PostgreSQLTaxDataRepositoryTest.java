package com.example.infrastructure;

import com.example.domain.PositionId;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for PostgreSQLTaxDataRepository.
 * Requires a running PostgreSQL database with test data.
 *
 * Run these tests with: mvn test -Dtest=PostgreSQLTaxDataRepositoryTest
 * Make sure PostgreSQL is running: docker-compose -f sql/docker-compose-postgresql.yml up -d
 */
public class PostgreSQLTaxDataRepositoryTest {

    private PostgreSQLTaxDataRepository repository;

    @BeforeEach
    void setUp() {
        var config = ConfigFactory.parseString("""
            tax-processing.database {
                host = "localhost"
                port = 5432
                database = "TaxProcessing"
                username = "taxuser"
                password = "TaxProcessing123!"
                pool {
                    initial-size = 1
                    max-size = 5
                    max-idle-time = "5 minutes"
                    max-lifetime = "30 minutes"
                    max-acquire-time = "30 seconds"
                    max-create-connection-time = "30 seconds"
                }
            }
            """);

        var connectionFactory = DatabaseConfiguration.createConnectionFactory(config);
        repository = new PostgreSQLTaxDataRepository(connectionFactory);
    }

    @Test
    @Disabled("Requires running PostgreSQL database with test data")
    void testLoadOpeningBalancesBatch() {
        // Test loading opening balances
        var openingBalances = repository.loadOpeningBalancesBatch("2023", 0, 10);

        assertThat(openingBalances).isNotNull();
        assertThat(openingBalances.size()).isLessThanOrEqualTo(10);

        // If there are results, verify structure
        if (!openingBalances.isEmpty()) {
            var balance = openingBalances.get(0);
            assertThat(balance.accountId()).isNotNull();
            assertThat(balance.instrumentId()).isNotNull();
            assertThat(balance.openingUnits()).isNotNull();
            assertThat(balance.openingCost()).isNotNull();
        }
    }

    @Test
    @Disabled("Requires running PostgreSQL database with test data")
    void testLoadTransactionsForPositions() {
        // First get some opening balances to get position IDs
        var openingBalances = repository.loadOpeningBalancesBatch("2023", 0, 5);

        if (openingBalances.isEmpty()) {
            return; // Skip test if no data
        }

        var positionIds = openingBalances.stream()
            .map(balance -> new PositionId(balance.accountId(), balance.instrumentId()))
            .toList();

        // Test loading transactions for these positions
        var transactions = repository.loadTransactionsForPositions(positionIds, "2023", 0, 10);

        assertThat(transactions).isNotNull();

        // If there are results, verify structure
        if (!transactions.isEmpty()) {
            var transaction = transactions.get(0);
            assertThat(transaction.id()).isNotNull();
            assertThat(transaction.accountId()).isNotNull();
            assertThat(transaction.instrumentId()).isNotNull();
            assertThat(transaction.type()).isNotNull();
            assertThat(transaction.dateTime()).isNotNull();
            assertThat(transaction.units()).isNotNull();
            assertThat(transaction.price()).isNotNull();
            assertThat(transaction.fees()).isNotNull();
        }
    }

    @Test
    @Disabled("Requires running PostgreSQL database with test data")
    void testCountOpeningBalances() {
        var count = repository.countOpeningBalances("2023");
        assertThat(count).isGreaterThanOrEqualTo(0);
    }

    @Test
    @Disabled("Requires running PostgreSQL database with test data")
    void testCountTransactionsForPositions() {
        // First get some opening balances to get position IDs
        var openingBalances = repository.loadOpeningBalancesBatch("2023", 0, 5);

        if (openingBalances.isEmpty()) {
            return; // Skip test if no data
        }

        var positionIds = openingBalances.stream()
            .map(balance -> new PositionId(balance.accountId(), balance.instrumentId()))
            .toList();

        var count = repository.countTransactionsForPositions(positionIds, "2023");
        assertThat(count).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testEmptyPositionIds() {
        // Test with empty position IDs list
        var transactions = repository.loadTransactionsForPositions(List.of(), "2023", 0, 10);
        assertThat(transactions).isEmpty();

        var count = repository.countTransactionsForPositions(List.of(), "2023");
        assertThat(count).isEqualTo(0);
    }
}