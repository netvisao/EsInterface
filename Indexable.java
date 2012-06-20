package com.a.search;

import java.util.Map;

public interface Indexable {
	public String getId();
	public Map<String, Object> toMap();
}
