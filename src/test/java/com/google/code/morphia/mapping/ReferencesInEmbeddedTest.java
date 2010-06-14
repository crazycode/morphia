package com.google.code.morphia.mapping;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;

/**
 * @author josephpachod
 */
public class ReferencesInEmbeddedTest extends TestBase
{
    @Entity
    static class Container extends TestEntity
    {
        String name ;
        @Embedded
        private EmbedContainingReference embed;
    }
    
    static class EmbedContainingReference {
        String name ;
        @Reference
        private ReferencedEntity ref;
        
        @Reference(lazy=true)
        private ReferencedEntity lazyRef;
    }
    
    @Entity
    static class ReferencedEntity extends TestEntity{
        String foo;
    }
    @Test
    public void testMapping() throws Exception {
        morphia.map(Container.class);
        morphia.map(ReferencedEntity.class);
    }
    
    @Test
    public void testNonLazyReferencesInEmbebbed() throws Exception {
        Container container = new Container();
        container.name= "nonLazy";
        ds.save(container);
        ReferencedEntity referencedEntity = new ReferencedEntity();
        ds.save(referencedEntity);
        
        container.embed = new EmbedContainingReference();
        container.embed.ref = referencedEntity;
        ds.save(container);
        
        Container reloadedContainer = ds.get(container);
        Assert.assertNotNull(reloadedContainer);
    }
    @Test
    public void testLazyReferencesInEmbebbed() throws Exception {
        Container container = new Container();
        container.name="lazy";
        ds.save(container);
        ReferencedEntity referencedEntity = new ReferencedEntity();
        ds.save(referencedEntity);
        
        container.embed = new EmbedContainingReference();
        container.embed.lazyRef = referencedEntity;
        ds.save(container);
        
        Container reloadedContainer = ds.get(container);
        Assert.assertNotNull(reloadedContainer);
    }
}
