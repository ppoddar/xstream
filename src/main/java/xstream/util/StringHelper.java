package xstream.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static string based utilities.
 *
 * @since 1.0
 *
 */
public class StringHelper {
	public static final char SINGLE_QUOTE = '\'';
	public static final char DOUBLE_QUOTE = '"';
	public static final char SPACE = ' ';
	
    public static List<String> toList(char separator, String s) {
        String[] tokens = s.split(""+separator);
        List<String> list = new ArrayList<String>(tokens.length);
        for (String t : tokens) {
            if (t == null) continue;
            list.add(t.trim());
        }
        return list;
    }
    
    public static String[] toArray(char separator, String s) {
        List<String> list = toList(separator, s);
        return list.toArray(new String[list.size()]);
    }


	public static boolean isEmpty(String s) {
		return s == null || s.trim().length() == 0;
	}
	
	/**
	 * Joins the given string with the given separator in between.
	 * @param separator a separator character
	 * @param elements elements to be joined
	 * @return a buffer, never null
	 */
	public static StringBuilder join(char separator, Object[] elements) {
		StringBuilder buf = new StringBuilder();
		if (elements == null || elements.length == 0)
			return buf;
		int i = 0;
		for (; i < elements.length-1; i++)
			buf.append(elements[i]).append(separator);
		
		buf.append(elements[i]);
		
		return buf;
	}
	
	
	/**
	 * Finds the position of the given key in the given collection.
	 * The position is defined as the iteration order of the collection. 
	 * @param coll collection of string
	 * @param key the string to search for
	 * @param ignoreCase if true, string elements are compared ignoring case
	 * @return 0-based index of the key in the given collection. 
	 * -1 if key is not found.
	 */
	public static int findIndex(String key, Collection<String> coll, boolean ignoreCase) {
	  int i = 0;
	  for (String s : coll) {
	    boolean found = ignoreCase ? key.equalsIgnoreCase(s) : key.equals(s);
	    if (found) return i;
	    i++;
	  }
	  return -1;
	}
	
	public static String unenclose(String s, char c) {
		return unenclose(s, c, c);
	}
	
	public static String unenclose(String s, char start, char end) {
		if (!isEnclosed(s, start, end))
			return s;
		int L = s.length();
		if ((s.charAt(0) == start) && (s.charAt(L-1) == end)) 
			return s.substring(1,L-1);
		return s;
	}
	
	public static String enclose(String s, char c) {
		return enclose(s, c, c);
	}
	
	public static String enclose(String s, char start, char end) {
		if (s == null)
			return s;
		return start + s + end;
	}
	
	public static String quoteSingle(String s) {
		return enclose(s, SINGLE_QUOTE);
	}
	public static String quote(String s) {
		return enclose(s, DOUBLE_QUOTE);
	}

	
	public static boolean isEnclosed(String s, char start, char end) {
		if ((s == null) || (s.length() < 2) )
			return false;
		int L = s.length();
		return(s.charAt(0) == start) && (s.charAt(L-1) == end);
	}
	
	public static boolean isEnclosed(String s, char c) {
		return isEnclosed(s, c, c);
	}
	public static boolean isDoubleQuoted(String s) {
		return isEnclosed(s, DOUBLE_QUOTE);
	}
	public static boolean isSingleQuoted(String s) {
		return isEnclosed(s, SINGLE_QUOTE);
	}
	public static boolean isQuoted(String s) {
		return isSingleQuoted(s) || isDoubleQuoted(s);
	}
	
	


	
	/**
	 * Convert all keys of the map to lower case such that keys
	 * will match ignoring case.
	 * @param map any map with String keys
	 * @return similar map but with lower case keys
	 */
	public static Map<String,?> makeMapIgnoreCase(Map<String,?> map) {
		Map<String,Object> result = new HashMap<String, Object>();
		for (String k : map.keySet()) {
			result.put(k.toLowerCase(), map.get(k));
		}
		return result;
	}
}
