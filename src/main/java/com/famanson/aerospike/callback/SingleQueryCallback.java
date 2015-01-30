package com.famanson.aerospike.callback;

import com.aerospike.client.Record;

public interface SingleQueryCallback<T> {
    T process(Record record);
}
