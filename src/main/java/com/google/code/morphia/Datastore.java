package com.google.code.morphia;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.google.code.morphia.utils.IndexDirection;
import com.mongodb.DB;
import com.mongodb.DBRef;
import com.mongodb.Mongo;
/**
 * Datastore interface to get/delete/save objects
 * @author Scott Hernandez
 */
public interface Datastore {	
	/** Creates a reference to the entity (using the current DB -can be null-, the collectionName, and id) */
	<T,V> DBRef createRef(Class<T> clazz, V id);
	/** Creates a reference to the entity (using the current DB -can be null-, the collectionName, and id) */
	<T> DBRef createRef(T entity);
	
	/** Creates a (type-safe) reference to the entity */
	<T> Key<T> getKey(T entity);
	
	/** Deletes the given entity (by id) */
	<T,V> void delete(Class<T> clazz, V id);
	/** Deletes the given entities (by id) */
	<T,V> void delete(Class<T> clazz, Iterable<V> ids);
	/** Deletes the given entities based on the query */
	<T> void delete(Query<T> q);
	/** Deletes the given entity (by id) */
	<T> void delete(T entity);

	/** Find all instances by type */
	<T> Query<T> find(Class<T> clazz);

	/** 
	 * <p>
	 * Find all instances by collectionName, and filter property.
	 * </p><p>
	 * This is the same as: {@code find(clazzOrEntity).filter(property, value); }
	 * </p>
	 */
	<T, V> Query<T> find(Class<T> clazz, String property, V value);
	
	/** 
	 * <p>
	 * Find all instances by collectionName, and filter property.
	 * </p><p>
	 * This is the same as: {@code find(clazzOrEntity).filter(property, value).offset(offset).limit(size); }
	 * </p>
	 */
	<T,V> Query<T> find(Class<T> clazz, String property, V value, int offset, int size);

	/** Find the given entities (by id); shorthand for {@code find("_id in", ids)} */
	<T,V> Query<T> get(Class<T> clazz, Iterable<V> ids);
	/** Find the given entities (by id); shorthand for {@code find("_id in", ids)} */
	<T> Query<T> getByKeys(Class<T> clazz, Iterable<Key<T>> ids);
	/** Find the given entity (by id); shorthand for {@code find("_id ", id)} */
	<T,V> T get(Class<T> clazz, V id);

	/** Find the given entity (by collectionName/id); think of this as refresh */
	<T> T get(T entity);
	
	/** Find the given entity (by collectionName/id);*/
	<T> T get(Class<T> clazz, DBRef ref);
	/** Find the given entity (by collectionName/id);*/
	<T> T get(Class<T> clazz, Key<T> key);


	/** Gets the count this kind of element*/
	<T> long getCount(T entity);
	/** Gets the count this kind of element*/
	<T> long getCount(Class<T> clazz);

	/** Gets the count of items returned by this query; same as {@code query.countAll()}*/
	<T> long getCount(Query<T> query); 
	
	/** Saves the entities (Objects) and updates the @Id, @CollectionName fields */
	<T> Iterable<Key<T>> save(Iterable<T> entities);
	/** Saves the entities (Objects) and updates the @Id, @CollectionName fields */
	<T> Iterable<Key<T>> save(T... entities);
	/** Saves the entity (Object) and updates the @Id, @CollectionName fields */
	<T> Key<T> save(T entity);

	/** updates all entities found with the operations; this is an atomic operation per entity*/
	<T> void update(Query<T> query, UpdateOperations ops);
	/** updates all entities found with the operations, if nothing is found insert the update as an entity; this is an atomic operation per entity*/
	<T> void update(Query<T> query, UpdateOperations ops, boolean createIfMissing);
	/** updates the first entity found with the operations; this is an atomic operation*/
	<T> void updateFirst(Query<T> query, UpdateOperations ops);
	
	/** The builder for all update operations */
	UpdateOperations ops();
	
	/** The instance this Datastore is using */
	DB getDB();
	/** The instance this Datastore is using */
	Mongo getMongo();
	/** The instance this Datastore is using */
	Mapper getMapper();

	/** Ensures (creating if necessary) the index and direction */
	<T> void ensureIndex(Class<T> clazz, String name, IndexDirection dir);
	/** Ensures (creating if necessary) the index and direction */
	<T> void ensureIndex(T entity, String name, IndexDirection dir);
	/** Ensures (creating if necessary) the indexes found during class mapping (using {@code @Indexed)}*/
	void ensureIndexes();
	/** Ensures (creating if necessary) the indexes found during class mapping (using {@code @Indexed)}*/
	<T> void ensureIndexes(Class<T>  clazz);
	/** ensure capped dbcollections for {@link Entity}s */
	void ensureCaps();
}