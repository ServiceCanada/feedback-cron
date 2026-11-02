package ca.gc.tbs;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.env.MockEnvironment;

class MainTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "mongodb://localhost:27017",
        "mongodb://127.0.0.1:27017/pagesuccess",
        "mongodb://mongodb:27017",
        "mongodb://LOCALHOST"
    })
    void localModeAcceptsLocalMongo(String uri) {
        assertDoesNotThrow(() -> Main.requireLocalMongo(uri));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "mongodb://pagesuccess.mongo.cosmos.azure.com:10255/?ssl=true",
        "mongodb+srv://cluster0.example.net",
        "mongodb://host1:27017,host2:27017",
        "mongodb://localhost.example.com",
        "not a uri"
    })
    void localModeRefusesAnyOtherMongo(String uri) {
        assertThrows(IllegalStateException.class, () -> Main.requireLocalMongo(uri));
    }

    @Test
    void prodIsNotLimitedToLocalMongo() {
        MockEnvironment env = new MockEnvironment()
                .withProperty("spring.data.mongodb.uri", "mongodb://pagesuccess.mongo.cosmos.azure.com:10255");

        assertDoesNotThrow(() -> Main.checkLocalMode(env));
    }

    @Test
    void localModeChecksTheConfiguredMongo() {
        MockEnvironment env = new MockEnvironment()
                .withProperty("app.integrations.enabled", "false")
                .withProperty("spring.data.mongodb.uri", "mongodb://pagesuccess.mongo.cosmos.azure.com:10255");

        assertThrows(IllegalStateException.class, () -> Main.checkLocalMode(env));
    }
}
