package org.example.utils;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Getter
@Builder
public class MongoConfig {
    @NonNull
    private final String host;
    private final int port;
    @NonNull
    private final String username;
    @NonNull
    private final String password;
    @NonNull
    private final String databaseName;
}
