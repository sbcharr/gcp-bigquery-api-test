package com.github.sbcharr.bigquery;

public enum StreamType {
    DEFAULT_SYNC("default_sync"),
    DEFAULT_ASYNC("default_async"),
    INSERT_ALL("insert_all");

    private String value;

    StreamType(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value;
    }
}
