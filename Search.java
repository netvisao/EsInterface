package com.a.search;



public interface Search<T> {
	 public boolean indexExists(String idxName);
	 public void upsert(T indexee);
	 public void remove(String _id);
     public void asyncUpsert(T indexee);
     public void asyncRemove(String _id);
	 public long countEqual(String queryTerm, String queryValue); 
}
