package com.famanson.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.famanson.aerospike.callback.FilteredQueryCallback;
import com.famanson.aerospike.callback.BatchQueryCallback;
import com.famanson.aerospike.callback.SingleQueryCallback;
import java.util.List;

public class AerospikeHelper {
    private AerospikeClient aerospikeClient;

    public AerospikeHelper(AerospikeClient aerospikeClient) {
        this.aerospikeClient = aerospikeClient;
    }

    public <T> T executeBatch(String namespace, String setName,
                              List<String> keyStrList, BatchQueryCallback<T> batchQueryCallback) {
        // This cannot be done with AQL! Sad days...
        Key[] keys = new Key[keyStrList.size()];
        int i=0;
        for (String id : keyStrList) {
            keys[i]= new Key(namespace, setName, id);
            i++;
        }
        Record[] records = aerospikeClient.get(new BatchPolicy(), keys);
        return batchQueryCallback.process(keyStrList, records);
    }

    @SuppressWarnings("unchecked")
    public <T> T executeSingleValue(String namespace, String setName, String keyStr, String valueName) {
        // No need to complicate things, get it straight from the key-value store
        Key key = new Key(namespace, setName, keyStr);
        return (T) aerospikeClient.get(new Policy(), key).getValue(valueName);
    }

    @SuppressWarnings("unchecked")
    public <T> T executeSingleValue(String namespace, String setName, String keyStr, SingleQueryCallback<T> singleQueryCallback) {
        // No need to complicate things, get it straight from the key-value store
        Key key = new Key(namespace, setName, keyStr);
        return singleQueryCallback.process(aerospikeClient.get(new Policy(), key));
    }

    public <T> T executeByFilters(String namespace, String setName,
                                   String[] binNames, Filter[] filters,
                                   FilteredQueryCallback<T> filteredQueryCallback) {
        // Indices is best queried by AQL
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setBinNames(binNames);
        stmt.setFilters(filters);
        RecordSet recordSet = null;
        try {
            recordSet = aerospikeClient.query(null, stmt);
            return filteredQueryCallback.process(recordSet);
        } finally {
            if (recordSet != null) {
                recordSet.close();
            }
        }
    }
}
