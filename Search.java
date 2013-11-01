package com.aol.identity.search;


import com.aol.interfaces.user.user_service_search_types.v2.SearchOperator;

import java.util.List;
import java.util.Map;

public interface Search<T> {



    public static class Term {
        private String name;
        private String value;
        private String path;
        private boolean isRelation;
        private SearchOperator operator;

        public Term(String name, String value) {
            this.name = name;
            this.value = value;
            this.operator = SearchOperator.AND;
            this.isRelation = false;
        }

        public Term(String name, String value, SearchOperator operator) {
            this.name = name;
            this.value = value;
            this.operator = operator;
            this.isRelation = false;
        }

        public Term(String name, String value, SearchOperator operator, boolean isRelation, String path) {
            this(name, value, operator);
            this.isRelation = isRelation;
            this.path = path;
        }

        public Term(String name, String value, boolean isRelation, String path) {
            this(name, value);
            this.isRelation = isRelation;
            this.path = path;
        }



        protected String getName() {
            return name;
        }
        protected String getValue() {
            return value;
        }
        protected String getPath() {
            return path;
        }
        protected SearchOperator getOperator() {
            return operator;
        }
        protected boolean isRelation() {
            return isRelation;
        }

        public String toString() {
            return "(" + name + ", " + value + ", " + path + ", " + isRelation + ", " + operator + ")";
        }
    }

    int LIMIT_MAX = 1000;
    int LIMIT_DEFAULT = 50;

	public boolean indexExists(String idxName);
    public boolean registerRelation(String elementQualifier);
	public boolean upsert(T indexee);
	public boolean remove(String _id);
    public void asyncUpsert(T indexee);
    public void asyncRemove(String _id);
	public long countEqual(Term queryTerm);
    public List<T> lookup(List<Term> terms, Integer limit,  Map<String, ?> constrains, Class<T> impl);
    public void postProcessSetup(PostActivity... dosAfter);
}
