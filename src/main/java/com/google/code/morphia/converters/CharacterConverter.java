/**
 * 
 */
package com.google.code.morphia.converters;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
@SuppressWarnings("unchecked")
public class CharacterConverter extends TypeConverter {	
	public CharacterConverter() {
		super(Character.class, char.class);
	}
	
	@Override
	public
	Object decode(Class targetClass, Object fromDBObject, MappedField optionalExtraInfo) throws MappingException {
		//TODO: Check length. Maybe "" should be null?
		return fromDBObject.toString().charAt(0);
	}
	
	@Override
	public
	Object encode(Object value, MappedField optionalExtraInfo) {
		return String.valueOf(value);
	}
}
