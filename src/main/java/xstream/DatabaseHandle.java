package xstream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import oracle.kv.KVStore;
import xstream.util.Assert;
import xstream.util.NoSQLURL;

/**
 * A handle to a database.
 * The handle is serializable and can be transmitted across processes.
 *  
 * @author pinaki poddar
 *
 */
abstract class DatabaseHandle implements Externalizable {
    private String _url;
    private transient KVStore _store;
    
    /**
     * required for serialization.
     */
    public DatabaseHandle() {
    }
    
    /**
     * Create a handle from the given string in URI syntx.
     * 
     * @param url a database URL.
     */
    protected DatabaseHandle(String url) {
        Assert.assertTrue(NoSQLURL.isValid(url), "invalid URL " + url);
        _url = url;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(getURL());
    }

    /**
     * Reads the URI from given input.
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _url = in.readUTF();

    }
    
    /**
     * gets URI to the database represented by this handle.
     * @return a string in URI format.
     */
    public final String getURL() {
        return _url;
    }
    
    /**
     * Gets database connection represented by this handle. 
     * @return a connection to the database
     */
    public final KVStore getStore() {
        if (_store == null) {
            _store = connectToStore(_url);
        }
        return _store;
    }
    
    protected KVStore connectToStore(String url) {
        if (url == null) return null;
        return  new NoSQLURL(url).openStore();
    }
    

}
