package com.google.code.morphia.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.code.morphia.DatastoreImpl;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class OrBuilder<T> implements Iterable<DBObject> {
	private static final long serialVersionUID = 1L;
	
	private DBCollection dbColl;
	private DatastoreImpl ds;
	private Class<T> clazz;
	
	private List<Query<T>> queries;

	public OrBuilder(Class<T> clazz, DBCollection dbColl, DatastoreImpl ds) {
		this.queries = new ArrayList<Query<T>>();

		this.dbColl = dbColl;
		this.ds = ds;
		this.clazz = clazz;
	}

	public OrBuilder<T> add(Query<T> query) {
		this.queries.add(query);
		return this;
	}
	
	public Query<T> add() {
		Query<T> new_query = new QueryImpl<T>(this.clazz, this.dbColl, this.ds);
		this.queries.add(new_query);
		
		return new_query;
	}

	@Override
	public Iterator<DBObject> iterator() {
		List<DBObject> db_objects = new ArrayList<DBObject>();
		
		for (Query<T> query: this.queries) {
			db_objects.add(query.getQueryObject());
		}
		
		return db_objects.iterator();
	}
}
