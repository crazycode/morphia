package com.google.code.morphia;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.code.morphia.MappedClass.MappedField;
import com.google.code.morphia.annotations.CappedAt;
import com.google.code.morphia.annotations.Indexed;
import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.utils.IndexDirection;
import com.google.code.morphia.utils.Key;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.Mongo;

/**
 * A generic (type-safe) wrapper around mongodb collections
 * 
 * @author Scott Hernandez
 */
@SuppressWarnings("unchecked")
public class DatastoreImpl implements SuperDatastore {
    private static final Logger log = Logger.getLogger(DatastoreImpl.class.getName());

	protected Morphia morphia;
	protected Mongo mongo;
	protected String dbName;

	public DatastoreImpl(Morphia morphia, Mongo mongo) {
		this(morphia, mongo, null);
	}
	
	public DatastoreImpl(Morphia morphia, Mongo mongo, String dbName) {
		this.morphia = morphia; this.mongo = mongo; this.dbName = dbName;
	}

	protected Object asObjectIdMaybe(Object id) {
		return Mapper.asObjectIdMaybe(id);
	}

	@Override
	public <T, V> DBRef createRef(Class<T> clazz, V id) {
		if (id == null) throw new MappingException("Could not get id for " + clazz.getName());
		return new DBRef(getDB(), getCollection(clazz).getName(), id);
	}

	@Override
	public <T> DBRef createRef(T entity) {
		Object id = getId(entity);
		if (id == null) throw new MappingException("Could not get id for " + entity.getClass().getName());
		return createRef(entity.getClass(), id);
	}
	
	protected <T,V> void delete(DBCollection dbColl, V id) {
		dbColl.remove(BasicDBObjectBuilder.start().add(Mapper.ID_KEY, asObjectIdMaybe(id)).get());
	}
	
	@Override
	public <T,V> void delete(String kind, V id) {
		DBCollection dbColl = mongo.getDB(dbName).getCollection(kind);
		delete(dbColl, id);
	}

	@Override
	public <T,V> void delete(Class<T> clazz, V id) {
		DBCollection dbColl = getCollection(clazz);
		delete(dbColl, id);
	}
	
	@Override
	public <T, V> void delete(Class<T> clazz, Iterable<V> ids) {
		//TODO: see about batching deletes
		for(V id : ids)
			delete(clazz, id);
	}

	@Override
	public <T> void delete(T entity) {
		try {
			Object id = getId(entity);
			delete(entity.getClass(), id);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected <T> void ensureIndex(String name, Class<T> clazz, String fieldName, IndexDirection dir, boolean unique, boolean dropDupsOnCreate) {
		BasicDBObjectBuilder keys = BasicDBObjectBuilder.start();
		BasicDBObjectBuilder keyOpts= null;
		
		if(dir == IndexDirection.BOTH)
			keys.add(fieldName, 1).add(fieldName, -1);
		else
			keys.add(fieldName, (dir == IndexDirection.ASC)? 1 : -1);

		if (name != null && !name.isEmpty()) {
			if (keyOpts == null) keyOpts = new BasicDBObjectBuilder();
			keyOpts.add("name", name);
		}
		if (unique) {
			if (keyOpts == null) keyOpts = new BasicDBObjectBuilder();
			keyOpts.add("unique", true);
			if (dropDupsOnCreate) keyOpts.add("dropDups", true);
		}
		
		DBCollection dbColl = getCollection(clazz);
		log.fine("Ensuring index for " + dbColl.getName() + "." + fieldName + " with keys " + keys);
		if (keyOpts == null) {
			log.fine("Ensuring index for " + dbColl.getName() + "." + fieldName + " with keys " + keys);
			dbColl.ensureIndex(keys.get());
		}else {
			log.fine("Ensuring index for " + dbColl.getName() + "." + fieldName + " with keys " + keys + " and opts " + keyOpts);
			dbColl.ensureIndex(keys.get(), keyOpts.get());
		}
	}
	
	@Override
	public <T> void ensureIndex(Class<T> entity, String name, IndexDirection dir) {
		ensureIndex(null, entity, name, dir, false, false);
	}
	@Override
	public <T> void ensureIndex(T entity, String name, IndexDirection dir) {
		ensureIndex(entity.getClass(), name, dir);
	}
	
	@Override
	public void ensureSuggestedIndexes() {
		//TODO loop over mappedClasses and call ensureIndex for each one on non-embedded objects (for now)
		for(MappedClass mc : morphia.getMappedClasses().values()){
			for(MappedField mf : mc.persistenceFields){
				if(mf.hasAnnotation(Indexed.class)) {
					Indexed index = mf.getAnnotation(Indexed.class);
					ensureIndex(index.name(), mc.clazz, mf.name,index.value(), index.unique(), index.dropDups());
				}
			}
		}
	}

	@Override
	public void ensureCaps() {
		Mapper mapr = morphia.getMapper();
		for(MappedClass mc : mapr.getMappedClasses().values())
			if (mc.entityAn != null && mc.entityAn.cap().value() > 0) {
				CappedAt cap = mc.entityAn.cap();
				String collName = mapr.getCollectionName(mc.clazz);
				BasicDBObjectBuilder dbCapOpts = BasicDBObjectBuilder.start("capped", true);
				if(cap.value() > 0) dbCapOpts.add("size", cap.value());
				if(cap.count() > 0) dbCapOpts.add("max", cap.count());
				DB db = mongo.getDB(dbName);
				if (db.getCollectionNames().contains(collName)) {
					//TODO: check if the dbCollection is already cap'd
//					DBCollection dbColl = db.getCollection(collName);
					log.warning("DBCollection already exists with same name(" + collName + "), not creating cap'd version!");
				} else {
					mongo.getDB(dbName).createCollection(collName, dbCapOpts.get());
					log.fine("Created cap'd DBCollection (" + collName + ") with opts " + dbCapOpts);
				}
			}	
	}

	@Override
	public <T> Query<T> find(String kind, Class<T> clazz){
		return new QueryImpl<T>(clazz, mongo.getDB(dbName).getCollection(kind), this);		
	}

	@Override
	public <T> Query<T> find(Class<T> clazz) {
		return new QueryImpl<T>(clazz, getCollection(clazz), this);
	}

	@Override
	public <T,V> Query<T> find(Class<T> clazz, String property, V value) {
		Query<T> query = find(clazz);
		return query.filter(property, value);
	}
	
	@Override
	public <T,V> Query<T> find(String kind, Class<T> clazz, String property, V value, int offset, int size) {		
		Query<T> query = find(kind, clazz);
		query.offset(offset); query.limit(size);
		return query.filter(property, value);
	}
	
	@Override
	public <T,V> Query<T> find(Class<T> clazz, String property, V value, int offset, int size) {
		Query<T> query = find(clazz);
		query.offset(offset); query.limit(size);
		return query.filter(property, value);
	}
	
	@Override
	public <T> T get(Class<T> clazz, DBRef ref) {
		return morphia.fromDBObject(clazz, ref.fetch());
	}
	
	@Override
	public <T, V> Query<T> get(Class<T> clazz, Iterable<V> ids) {
		List objIds = new ArrayList();
		for (V id : ids) {
			objIds.add(asObjectIdMaybe(id));
		}
		return find(clazz, Mapper.ID_KEY + " in", objIds);
	}
	
	@Override
	public <T> Query<T> getByKeys(Class<T> clazz, Iterable<Key<T>> keys) {
		Mapper mapr = morphia.getMapper();
		String kind = mapr.getCollectionName(clazz);
		List objIds = new ArrayList();
		for (Key<T> key : keys) {
			if (!kind.equals(key.updateKind(mapr)))
				throw new RuntimeException("collection names don't match for key and class: " + kind + " != " + key.getKind());

			objIds.add(asObjectIdMaybe(key.getId()));
		}
		return find(clazz, Mapper.ID_KEY + " in", objIds);
	}

	@Override
	public <T,V> T get(String kind, Class<T> clazz, V id) {
		List<T> results = find(kind, clazz, Mapper.ID_KEY, id, 0, 1).asList();
		if (results == null || results.size() == 0) return null;
		return results.get(0);
	}

	@Override
	public <T, V> T get(Class<T> clazz, V id) {
		List<T> results = find(getCollection(clazz).getName(), clazz, Mapper.ID_KEY, id, 0, 1).asList();
		if (results == null || results.size() == 0) return null;
		return results.get(0);
	}

	@Override
	public <T> T get(Class<T> clazz, Key<T> key) {
		Mapper mapr = morphia.getMapper();
		String kind = mapr.getCollectionName(clazz);
		String keyKind = key.updateKind(mapr);
		if (!kind.equals(keyKind)) 
			throw new RuntimeException("collection names don't match for key and class: " + kind + " != " + keyKind);
		
		return get(clazz, key.getId());
	}
	
	@Override
	public <T> T get(T entity) {
		Object id = getId(entity);
		if (id == null) throw new MappingException("Could not get id for " + entity.getClass().getName());
		return (T) get(entity.getClass(), id);
	}

	public DBCollection getCollection(Class clazz) {
		String collName = morphia.getMapper().getCollectionName(clazz);
		return mongo.getDB(dbName).getCollection(collName);
	}
	public DBCollection getCollection(Object obj) {
		String collName = morphia.getMapper().getCollectionName(obj);
		return mongo.getDB(dbName).getCollection(collName);
	}
	
	@Override
	public <T> long getCount(T entity) {
		return getCollection(entity).getCount();
	}
	
	@Override
	public <T> long getCount(Class<T> clazz) {
		return getCollection(clazz).getCount();
	}
	
	@Override
	public long getCount(String kind) {
		return mongo.getDB(dbName).getCollection(kind).getCount();
	}

	@Override
	public <T> long getCount(Query<T> query) {
		return query.countAll();
	}

	@Override
	public DB getDB() {
		return (dbName == null) ? null : mongo.getDB(dbName);
	}
	protected Object getId(Object entity) {
		MappedClass mc;
		String keyClassName = entity.getClass().getName();
		if (morphia.getMappedClasses().containsKey(keyClassName))
			mc = morphia.getMappedClasses().get(keyClassName);
		else
			mc = new MappedClass(entity.getClass());
		
		try {
			return mc.idField.get(entity);
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Mongo getMongo() {
		return this.mongo;
	}

	@Override
	public Morphia getMorphia() {
		return this.morphia;
	}

	@Override
	public <T> Iterable<Key<T>> save(Iterable<T> entities) {
		ArrayList<Key<T>> savedKeys = new ArrayList<Key<T>>();
		//for now, do it one at a time.
		for(T ent : entities)
			savedKeys.add(save(ent));
		return savedKeys;

	}

	@Override
	public <T> Iterable<Key<T>> save(T...entities) {
		ArrayList<Key<T>> savedKeys = new ArrayList<Key<T>>();
		//for now, do it one at a time.
		for(T ent : entities)
			savedKeys.add(save(ent));
		return savedKeys;
	}
	
	protected <T> Key<T> save(DBCollection dbColl, T entity) {
		Mapper mapr = morphia.getMapper();
		MappedClass mc = mapr.getMappedClass(entity);
		DBObject dbObj = mapr.toDBObject(entity);
		dbColl.save(dbObj);
		mapr.updateKeyInfo(entity, dbObj.get(Mapper.ID_KEY), dbColl.getName());
		mc.callLifecycleMethods(PostPersist.class, entity, dbObj);
		return new Key<T>(dbColl.getName(), getId(entity));		
	}

	@Override
	public <T> Key<T> save(String kind, T entity) {	
		DBCollection dbColl = mongo.getDB(dbName).getCollection(kind);
		return save(dbColl, entity);
	}

	@Override
	public <T> Key<T> save(T entity) {
		DBCollection dbColl = getCollection(entity);
		return save(dbColl, entity);
    }	
}
