/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia;

import java.util.Date;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 *
 * @author Scott Hernandez
 */
public class TestPerf  extends TestBase{
	@Entity
	public static class Address {
		@Id String id;
		String name = "Scott";
		String street = "3400 Maple";
		String city = "Manhattan Beach";
		String state = "CA";
		int zip = 94114;
	}

	@Test @Ignore
    public void testAddressInsertPerf() throws Exception {
    	int count = 10000;
    	long startTicks = new Date().getTime();
    	insertAddresses(count, true);
    	long endTicks = new Date().getTime();
    	long rawInsertTime = endTicks - startTicks;
    	
    	ds.delete(ds.find(Address.class));
    	startTicks = new Date().getTime();
    	insertAddresses(count, false);
    	endTicks = new Date().getTime();
    	long insertTime = endTicks - startTicks;
    	
    	Assert.assertTrue("Insert(" + count + " addresses) performance is too slow: " + 
    				String.valueOf((double)insertTime/rawInsertTime).subSequence(0, 5) + "X slower", 
    			insertTime < (rawInsertTime * 1.1));
    }

    @Test @Ignore
    public void testAddressInsertThreadedPerf() throws Exception {
    	int count = 10000;

//    	ThreadPool<>
    	//TODO add thread pool here to test concurrency
    	long startTicks = new Date().getTime();
    	insertAddresses(count, true);
    	long endTicks = new Date().getTime();
    	long rawInsertTime = endTicks - startTicks;
    	
    	startTicks = new Date().getTime();
    	insertAddresses(count, false);
    	endTicks = new Date().getTime();
    	long insertTime = endTicks - startTicks;
    	
    	Assert.assertTrue("Insert(" + count + " addresses) performance is too slow: " + 
    				String.valueOf((double)insertTime/rawInsertTime).subSequence(0, 5) + "X slower", 
    			insertTime < (rawInsertTime * 1.1));
    }
    
    public void insertAddresses(int count, boolean raw) {
    	Address template = new Address();
    	DBCollection dbColl = db.getCollection(((DatastoreImpl)ds).getMapper().getCollectionName(Address.class));
    	for(int i=0;i<count;i++) {
    		if(raw) {
    			DBObject addr = new BasicDBObject();
    			addr.put("name", template.name);
    			addr.put("street", template.street);
    			addr.put("city", template.city);
    			addr.put("state", template.state);
    			addr.put("zip", template.zip);
    			dbColl.save(addr);
    		}else {
    			Address addr = new Address();
    			ds.save(addr);
    		}
    	}
    }
}
