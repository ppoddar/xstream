package xstream.util;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

@SuppressWarnings("serial")
public class NoSQLURL implements Serializable {
    private URI _url;
    
    private static final Logger _logger = Logger.getLogger(NoSQLURL.class.getName());
    public NoSQLURL(String uri) {
        try {
            _url = new URI(uri);
            validate(_url);
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public NoSQLURL(URI uri) {
        validate(uri);
        _url = uri;
        
    }
    
    public static boolean isValid(String uriString) {
        try {
            URI uri = new URI(uriString);
            validate(uri);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
    
    static void validate(URI uri) {
        Assert.assertTrue(uri.isAbsolute(), uri + " is not absolute");
        Assert.assertTrue("nosql".equals(getProtocol(uri)), "invalid protocol "
                + getProtocol(uri) + " . must be nosql");
        Assert.assertTrue(uri.getRawPath().split("/").length >= 2, "invalid "
                + " path " + uri.getPath());
        Assert.assertTrue(!StringHelper.isEmpty(getHost(uri)), "invalid "
                + " host " + getHost(uri));
    }
    
    public URI getURI() {
        try {
            return new URI(getProtocol(_url), _url.getRawUserInfo(), 
                    getHost(_url), getPort(),
                    _url.getPath(), _url.getRawQuery(), _url.getRawFragment());
        } catch (URISyntaxException ex) {
            return _url;
        }
    }
    
    public String getProtocol() {
        return getProtocol(_url);
    }
    public static String getProtocol(URI uri) {
        String scheme = uri.getScheme();
        return StringHelper.isEmpty(scheme) ? "nosql" : uri.getScheme();
    }
    
    public String getStoreName() {
        return _url.getPath().substring(1).split("/")[0];
    }
    
    public String getSeriesName() {
        return _url.getPath().substring(1).split("/")[1];
    }
    
//    public NoSQLURL setSeriesName(String name) {
//        String newURL = this.getProtocol() + "://" + this.getHost()
//          + ":" + this.getPort() + "/" + getStoreName() + "/" + name;
//        return new NoSQLURL(newURL);
//    }
    
    
    public int getPort() {
        return _url.getPort() < 0 ? 5000 : _url.getPort();
    }
    
    public String getHost() {
        return getHost(_url);
    }
    public static String getHost(URI uri) {
        return StringHelper.isEmpty(uri.getHost()) ? "localhost" : uri.getHost();
    }
    
    
    public KVStore openStore() {
        KVStoreConfig config = new KVStoreConfig(getStoreName(), 
                getHost(_url) + ':' + getPort());
//        config.setCheckInterval(timeout, unit);
//        config.setConsistency(consistency);
//        config.setDurability(durability);
//        config.setLOBChunkSize(chunkSize);
//        config.setLOBChunksPerPartition(lobChunksPerPartition);
//        config.setLOBSuffix(lobSuffix);
//        config.setLOBTimeout(timeout, unit);
//        config.setLOBVerificationBytes(lobVerificationBytes);
//        config.setRequestLimit(requestLimitConfig)
        _logger.log(Level.FINE, "connecting to " + config);
        KVStore store = KVStoreFactory.getStore(config);
        return store;
    }
    
    public String toString() {
        return _url.toString();
    }
    
    

}
