package org.sharkyaya.ner;

import static java.util.Collections.singletonList;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticSearchIndexSpecParser {
	
	static {
		System.out.println("===============================================================================");
		System.out.println("===================== Loading: ElasticSearchIndexSpecParser =========================");
		System.out.println("===============================================================================");

	}
	
    
    private final static Pattern INDEX_SPEC_RE = Pattern.compile("(?<indexname>[a-z][a-z_-]+):(?<label>[A-Za-z0-9_]+)\\((?<props>[^\\)]+)\\)");
    private final static Pattern PROPS_SPEC_RE = Pattern.compile("((?!=,)([A-Za-z0-9_]+))+");
    
    public static Map<String, List<ElasticSearchIndexSpec>> parseIndexSpec(String spec) throws ParseException {
    	
        if (spec == null) {
            return Collections.emptyMap();
        }
        Map<String, List<ElasticSearchIndexSpec>> map = new LinkedHashMap<>();
        Matcher matcher = INDEX_SPEC_RE.matcher(spec);
        try
        {
        while (matcher.find()) {

            Matcher propsMatcher = PROPS_SPEC_RE.matcher(matcher.group("props"));
            Set<String> props = new HashSet<String>();
            while (propsMatcher.find()) {
                props.add(propsMatcher.group());
            }
            
            String label = matcher.group("label");
            
            if (map.containsKey(label)) {
            	throw new ParseException(matcher.group(), 0);
            }
            map.put(label,singletonList(new ElasticSearchIndexSpec(matcher.group("indexname"), props)));
        }
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
        return map;
    }
    

}
