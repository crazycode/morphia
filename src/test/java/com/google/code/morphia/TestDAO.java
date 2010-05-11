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

import com.google.code.morphia.mapping.Constraints;
import com.google.code.morphia.mapping.Modifiers;
import com.google.code.morphia.testdaos.HotelDAO;
import com.google.code.morphia.testmodel.Address;
import com.google.code.morphia.testmodel.Hotel;
import com.google.code.morphia.testmodel.PhoneNumber;
import com.mongodb.DB;
import com.mongodb.Mongo;
import java.util.Date;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
public class TestDAO {

    @Test
    public void testNewDAO() throws Exception {
        Mongo mongo = new Mongo();
        DB db = mongo.getDB("morphia_test");
        try {
            Morphia morphia = new Morphia();
            morphia.map(Hotel.class);

            DAO<Hotel,String> hotelDAO = new DAO<Hotel,String>(Hotel.class, mongo, morphia, "morphia_test");

            Hotel borg = Hotel.create();
            borg.setName("Hotel Borg");
            borg.setStars(4);
            borg.setTakesCreditCards(true);
            borg.setStartDate(new Date());
            borg.setType(Hotel.Type.LEISURE);
            Address borgAddr = new Address();
            borgAddr.setStreet("Posthusstraeti 11");
            borgAddr.setPostCode("101");
            borg.setAddress(borgAddr);

            hotelDAO.save(borg);
            assertEquals(1, hotelDAO.count());
            assertNotNull(borg.getId());

            Hotel hotelLoaded = hotelDAO.get(borg.getId());
            assertEquals(borg.getName(), hotelLoaded.getName());
            assertEquals(borg.getAddress().getPostCode(), hotelLoaded.getAddress().getPostCode());

            Hotel hotelByValue = hotelDAO.findOne("name", "Hotel Borg");
            assertNotNull(hotelByValue);
            assertEquals(borg.getStartDate(), hotelByValue.getStartDate());

            assertTrue(hotelDAO.exists("stars", 4));

            Hotel hilton = Hotel.create();
            hilton.setName("Hilton Hotel");
            hilton.setStars(4);
            hilton.setTakesCreditCards(true);
            hilton.setStartDate(new Date());
            hilton.setType(Hotel.Type.BUSINESS);
            Address hiltonAddr = new Address();
            hiltonAddr.setStreet("Some street 44");
            hiltonAddr.setPostCode("101");
            hilton.setAddress(hiltonAddr);
            hilton.getPhoneNumbers().add(new PhoneNumber(354, 1234567, PhoneNumber.Type.PHONE));

            hotelDAO.save(hilton);

            List<Hotel> allHotels = hotelDAO.find().asList();
            assertEquals(2, allHotels.size());
            assertEquals(2, hotelDAO.findIds().asList().size());

            assertEquals(1, hotelDAO.find(new Constraints().skip(1).limit(10)).asList().size());
            assertEquals(1, hotelDAO.find(new Constraints().limit(1)).asList().size());
            assertTrue(hotelDAO.exists("type", Hotel.Type.BUSINESS));
            assertNotNull(hotelDAO.findOne("type", Hotel.Type.LEISURE));

            assertEquals(0, hotelDAO.count(new Constraints().field("stars").notEqualTo(4)));
            assertEquals(2, hotelDAO.count(new Constraints().field("stars").lessThan(5)));
            assertEquals(2, hotelDAO.count(new Constraints().field("stars").greaterThanOrEqualTo(4)));
            assertEquals(2, hotelDAO.count(new Constraints().field("stars").lessThan(5)));
            assertEquals(1, hotelDAO.count(new Constraints().field("phoneNumbers").size(1)));
            assertEquals(2, hotelDAO.count(new Constraints("stars", 4).orderBy("address.address_street")));
            assertEquals(borg.getName(), hotelDAO.find(new Constraints("stars", 4).orderBy("address.address_street")).next().getName());
            assertEquals(hilton.getName(), hotelDAO.find(new Constraints("stars", 4).orderByDesc("address.address_street")).next().getName());
            assertEquals(hilton.getName(), hotelDAO.find(new Constraints("stars", 4).orderBy("stars").orderByDesc("address.address_street")).next().getName());

            hotelDAO.deleteById(borg.getId());
            assertEquals(1, hotelDAO.count());

            hotelDAO.dropCollection();
            assertEquals(0, hotelDAO.count());

        } finally {
            db.dropDatabase();
        }
    }

    @Test
    public void testDAO() throws Exception {
        Mongo mongo = new Mongo();
        DB db = mongo.getDB("morphia_test");
        try {

            Morphia morphia = new Morphia();
            morphia.map(Hotel.class);

            Hotel borg = Hotel.create();
            borg.setName("Hotel Borg");
            borg.setStars(4);
            borg.setTakesCreditCards(true);
            borg.setStartDate(new Date());
            borg.setType(Hotel.Type.LEISURE);
            Address borgAddr = new Address();
            borgAddr.setStreet("Posthusstraeti 11");
            borgAddr.setPostCode("101");
            borg.setAddress(borgAddr);

            HotelDAO hotelDAO = new HotelDAO(morphia, mongo);
            hotelDAO.save(borg);
            assertEquals(1, hotelDAO.count());
            assertNotNull(borg.getId());

            Hotel hotelLoaded = hotelDAO.get(borg.getId());
            assertEquals(borg.getName(), hotelLoaded.getName());
            assertEquals(borg.getAddress().getPostCode(), hotelLoaded.getAddress().getPostCode());

            Hotel hotelByValue = hotelDAO.findOne("name", "Hotel Borg");
            assertNotNull(hotelByValue);
            assertEquals(borg.getStartDate(), hotelByValue.getStartDate());

            assertTrue(hotelDAO.exists("stars", 4));

            Hotel hilton = Hotel.create();
            hilton.setName("Hilton Hotel");
            hilton.setStars(4);
            hilton.setTakesCreditCards(true);
            hilton.setStartDate(new Date());
            hilton.setType(Hotel.Type.BUSINESS);
            Address hiltonAddr = new Address();
            hiltonAddr.setStreet("Some street 44");
            hiltonAddr.setPostCode("101");
            hilton.setAddress(hiltonAddr);

            hotelDAO.save(hilton);

            List<Hotel> allHotels = hotelDAO.find().asList();
            assertEquals(2, allHotels.size());

            assertEquals(1, hotelDAO.find(new Constraints().skip(1).limit(10)).asList().size());
            assertEquals(1, hotelDAO.find(new Constraints().limit(1)).asList().size());
            assertTrue(hotelDAO.exists("type", Hotel.Type.BUSINESS));
            assertNotNull(hotelDAO.findOne("type", Hotel.Type.LEISURE));

            // try updating
            Modifiers mods = new Modifiers().inc("stars", 1);
            hotelDAO.update(new Constraints("stars", 4), mods);
            assertEquals(2, hotelDAO.count(new Constraints("stars", 5)));

            hotelDAO.deleteById(borg.getId());
            assertEquals(1, hotelDAO.count());

            hotelDAO.dropCollection();
            assertEquals(0, hotelDAO.count());

        } finally {
            db.dropDatabase();
        }
    }
    @Test
    public void testSaveEntityWithId() throws Exception {
         Mongo mongo = new Mongo();
         DB db = mongo.getDB("morphia_test");
       
         try {
             Morphia morphia = new Morphia();
             HotelDAO hotelDAO = new HotelDAO(morphia, mongo);
            
             Hotel borg = Hotel.create();
             borg.setName("Hotel Borg");
             borg.setStars(4);
             hotelDAO.save(borg);
            
             Hotel hotelLoaded = hotelDAO.get(borg.getId());           
             hotelLoaded.setStars(5);
             hotelDAO.save(hotelLoaded);
             Hotel hotelReloaded = hotelDAO.get(borg.getId());
             assertEquals(5,hotelReloaded.getStars());
         } finally {
             db.dropDatabase();
         }
    }
    
    @Test @Ignore
    public void testErasureDao() throws Exception {
        Morphia morphia = new Morphia();
        Mongo mongo = new Mongo();
        DB db = mongo.getDB("morphia_test");
        //broken, you must subclass DAO to use this constructor.
        DAO<Hotel, String> hotelDAO = new DAO<Hotel, String>(mongo, morphia, db.getName());
        hotelDAO.find();
    }    
}
