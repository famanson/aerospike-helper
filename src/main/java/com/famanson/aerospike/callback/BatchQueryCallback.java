package com.famanson.aerospike.callback;

import com.aerospike.client.Record;
import java.util.List;

public interface BatchQueryCallback<T> {
    T process(List<String> keyList, Record[] records);
}
