package xstream.util;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.Version;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.WriteOptions;

/**
 * A read-modify-write or 'SELECT FOR UPDATE' lock for NoSQL distributed 
 * database.
 * 
 * 
 * @author pinaki poddar
 *
 */
public class RMWLock {
    
    private final long _lockTimeoutMs;
    private final int  _maxRetries;
    
    private static int READ_TIME_OUT_MS  = 1000;
    private static int WRITE_TIME_OUT_MS = 2000;
    private static int LOCK_TIME_OUT_MS  = 10*WRITE_TIME_OUT_MS;
    
    public static ReadOptions READ_OPTION = new ReadOptions(
            Consistency.ABSOLUTE, 
            READ_TIME_OUT_MS, TimeUnit.MILLISECONDS);
    public static WriteOptions WRITE_OPTION = new WriteOptions(
            Durability.COMMIT_SYNC, 
            WRITE_TIME_OUT_MS, TimeUnit.MILLISECONDS);

    /**
     * Creates a lock with default with lock time out.
     */
    public RMWLock() {
        this(LOCK_TIME_OUT_MS);
    }
    
    /**
     * Creates a lock with given with lock time out in milliseconds.
     * 
     * @param lockTimeoutMs maximum time to acquire a lock
     */
    public RMWLock(long lockTimeoutMs) {
        _lockTimeoutMs = lockTimeoutMs;
        _maxRetries = (int)Math.max(1, _lockTimeoutMs/WRITE_TIME_OUT_MS);
    }

    /**
     * Updates given row with given update function.
     * 
     * @param store connection to a store
     * @param row a row to be updated. The row may not even exist in database.
     * In such case, the must have all its primary key fields set.
     * @param updater an update function
     * @return the updated row
     */
    public Row update(KVStore store, Row row, Updater updater) {
        if (row == null) {
            throw new IllegalArgumentException("cannot update null row");
        }
        Version version = row.getVersion();
        if (version == null) { // row is not retrieved from data store
            updater.update(row);
            ReturnRow rr = row.getTable().createReturnRow(Choice.ALL);
            version = store.getTableAPI().put(row, rr, WRITE_OPTION);
            if (version == null) {
                throw new RuntimeException();
            } else {
                return row;
            }
        }
        return update(store, row, version, updater, _maxRetries);
    }

    /**
     * 
     * @param store
     *            connection to store
     * @param row
     *            record to be updated
     * @param latestVersion
     *            the version of the record to match for update
     * @param updater a function to update the row
     * @param attempt
     *            number of  attempts to update
     * @return the updated row
     */
    private Row update(KVStore store, Row row, Version version,
             Updater updater, int attempt) {
        if (attempt <= 0) {
            throw new RuntimeException("cannot lock row " + row + " in " + _lockTimeoutMs + "ms");
        }
        updater.update(row);
        ReturnRow rr = row.getTable().createReturnRow(Choice.ALL);
        version = store.getTableAPI().putIfVersion(row, version, rr, WRITE_OPTION);
        
        if (version == null) {
            if (rr.getVersion() == null) {
                throw new RuntimeException("putifversion failed and return row version is also null");
            }
            return update(store, rr, rr.getVersion(), updater, --attempt);
        } else {
            return row;
        }
    }
}
