package com.famanson.aerospike.callback;

import com.aerospike.client.query.RecordSet;

public interface FilteredQueryCallback<T> {
    T process(RecordSet recordSet);
}
