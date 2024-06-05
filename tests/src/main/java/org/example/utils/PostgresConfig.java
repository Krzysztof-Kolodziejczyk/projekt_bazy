package org.example.utils;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Getter
@Builder
public class PostgresConfig {
    @NonNull
    private final String url;
    @NonNull
    private final String username;
    @NonNull
    private final String password;
}
