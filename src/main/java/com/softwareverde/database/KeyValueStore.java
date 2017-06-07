package com.softwareverde.database;

public interface KeyValueStore {
    String getString(String key);
    void putString(String key, String value);

    boolean hasKey(String key);
    void removeKey(String key);

    void clear();
    void clearAllStores();
}
