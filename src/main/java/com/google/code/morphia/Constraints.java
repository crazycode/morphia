package com.google.code.morphia;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
public class Constraints implements Serializable {
   	private static final long serialVersionUID = 1L;

    private Sort sort;
    private int startIndex, resultSize;
    private Map<String,Object> query;
    private Map<String,Integer> fields;

    private String currentKey;

    public Constraints() {
        startIndex = -1;
        resultSize = -1;
        query = new HashMap<String,Object>();
        fields = new HashMap<String,Integer>();
    }
    
    public Constraints( String name, Object value ) {
        this();
        field(name).equalTo(value);
    }

    public Constraints orderByDesc( String name ) {
        return orderBy(name, false);
    }

    public Constraints orderBy( String name ) {
        return orderBy(name, true);
    }

    public Constraints orderBy( String name, boolean ascending ) {
        if ( sort == null ) {
            sort = new Sort();
        }
        sort.add(name, ascending);
        return this;
    }

    public Constraints skip( int resultsToSkip ) {
        startIndex = resultsToSkip;
        return this;
    }

    public Constraints limit( int batchSize ) {
        resultSize = batchSize;
        return this;
    }

    public Constraints field( String name ) {
        currentKey = name;
        return this;
    }

    public Constraints equalTo( Object value ) {
        return addField(value);
    }
    
    public Constraints notEqualTo( Object value ) {
        return addMapField(FilterOperator.NOT_EQUAL, value);
    }

    public Constraints lessThan( Object value ) {
        return addMapField(FilterOperator.LESS_THAN, value);
    }

    public Constraints lessThanOrEqualTo( Object value ) {
        return addMapField(FilterOperator.LESS_THAN_OR_EQUAL, value);
    }

    public Constraints greaterThan( Object value ) {
        return addMapField(FilterOperator.GREATER_THAN, value);
    }

    public Constraints greaterThanOrEqualTo( Object value ) {
        return addMapField(FilterOperator.GREATER_THAN_OR_EQUAL, value);
    }

    public Constraints exists() {
        return addMapField(FilterOperator.EXISTS, true);
    }

    public Constraints notExists() {
        return addMapField(FilterOperator.EXISTS, false);
    }

    public Constraints matches( String regExp, boolean caseInsensitive ) {
        Pattern pattern = caseInsensitive
                ? Pattern.compile(regExp, Pattern.CASE_INSENSITIVE)
                : Pattern.compile(regExp)
                ;
        return addField(pattern);
    }

    public Constraints size( int size ) {
        return addMapField(FilterOperator.SIZE, size);
    }

    public Constraints mod(int modulo, int result) {
        List<Integer> value = new ArrayList<Integer>();
        value.add(modulo);
        value.add(result);
        return addMapField(FilterOperator.MOD, value);
    }

    @SuppressWarnings("unchecked")
    public Constraints hasAnyOf( List values ) {
        return addMapField(FilterOperator.IN, values);
    }

    @SuppressWarnings("unchecked")
    public Constraints hasNoneOf( List values ) {
        return addMapField(FilterOperator.NOT_IN, values);
    }

    @SuppressWarnings("unchecked")
    public Constraints hasAllOf( List values ) {
        return addMapField(FilterOperator.ALL, values);
    }

    public Constraints where( String clause ) {
        query.put("$where", clause);
        return this;
    }

    /**
     * Specifiy that the field with the name supplied should be
     * returned in the results.
     *
     * @param name
     * @return
     */
    public Constraints include( String name ) {
        fields.put(name, 1);
        return this;
    }

    /**
     * Specifiy that the field with the name supplied should NOT be
     * returned in the results.
     *
     * @param name
     * @return
     */
    public Constraints exclude( String name ) {
        fields.put(name, 0);
        return this;
    }

    @SuppressWarnings("unchecked")
	private Constraints addField( Object value ) {
        validateField();
        query.put(currentKey, value.getClass().isEnum() ? ((Enum)value).name() : value);
        currentKey = null;
        return this;
    }

    @SuppressWarnings("unchecked")
    private Constraints addMapField( FilterOperator op, Object value ) {
        validateField();
        query.put(currentKey, map(op.val(), value.getClass().isEnum() ? ((Enum)value).name() : value));
        currentKey = null;
        return this;
    }

    private void validateField() {
        if ( currentKey == null ) {
            throw new IllegalArgumentException("Must specify field first");
        }
    }

    private Map<String,Object> map( String key, Object value ) {
        Map<String,Object> map = new HashMap<String,Object>();
        map.put(key, value);
        return map;
    }

    Map<String, Object> getQuery() {
        return query;
    }

    Map<String, Integer> getFields() {
        return fields;
    }

    int getResultSize() {
        return resultSize;
    }

    Sort getSort() {
        return sort;
    }

    int getStartIndex() {
        return startIndex;
    }
}
