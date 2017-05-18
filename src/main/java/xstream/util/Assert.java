package xstream.util;

public class Assert {
    public static void assertTrue(boolean condition) {
        assertTrue(condition, "");
    }
    
    public static void assertTrue(boolean condition, RuntimeException ex) {
        if (!condition) {
            throw ex;
        }
    }
    
    public static void assertTrue(boolean condition, String message) {
        assertTrue(condition, new RuntimeException(message));
    }

    public static void assertTrue(boolean condition, String message, Exception ex) {
        assertTrue(condition, new RuntimeException(message, ex));
    }
    
    public static void assertFalse(boolean condition, RuntimeException ex) {
        assertTrue(!condition, ex);
    }
    
    public static void assertFalse(boolean condition, String message) {
        assertTrue(!condition, message);
    }

    public static void assertFalse(boolean condition, String message, Exception ex) {
        assertTrue(!condition, message, ex);
    }
    

    
    public static void assertNull(Object obj, RuntimeException ex) {
        assertTrue(obj == null, ex);
    }
    
    public static void assertNull(Object obj, String message) {
        assertTrue(obj == null, message);
    }
    
    public static void assertNull(Object obj, String message, Exception ex) {
        assertTrue(obj == null, message, ex);
    }

    public static void assertNotNull(Object obj, RuntimeException ex) {
        assertTrue(obj != null, ex);
    }

    public static void assertNotNull(Object obj, String message) {
        assertTrue(obj != null, message);
    }
    
    public static void assertNotNull(Object obj, String message, Exception ex) {
        assertTrue(obj != null, message, ex);
    }

}
