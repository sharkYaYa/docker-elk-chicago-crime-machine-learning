package org.sharkyaya.ner;

import java.util.LinkedHashSet;
import java.util.Set;

class ElasticSearchIndexSpec {
	
	
	static {
		System.out.println("===============================================================================");
		System.out.println("===================== Loading: ElasticSearchIndexSpec =========================");
		System.out.println("===============================================================================");

	}
	
	

    private String indexName;
	private Set<String> properties = new LinkedHashSet<String>();
    
    public ElasticSearchIndexSpec(String indexName, Set<String> properties) {
        this.indexName = indexName;
        this.properties = properties;
    }
    
    public String getIndexName() {
		return indexName;
	}
    
    public Set<String> getProperties() {
		return properties;
	}

	public String toString() {
        String s = this.getClass().getSimpleName() + " " + indexName + ": (";
        for (String p: properties) {
            s += p + ",";
        }
        s += ")";
        return s;
    }
}
