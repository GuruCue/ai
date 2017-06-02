/*
 * This file is part of Guru Cue Search & Recommendation Engine.
 * Copyright (C) 2017 Guru Cue Ltd.
 *
 * Guru Cue Search & Recommendation Engine is free software: you can
 * redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * Guru Cue Search & Recommendation Engine is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Guru Cue Search & Recommendation Engine. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.gurucue.recommendations.recommender.dto;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import gnu.trove.set.TIntSet;

public class StringAttr extends Attr implements Cloneable {
    String value;
    int maxLength;
    
    public StringAttr(String name, int maxLength) {
        super(name);
        this.maxLength = maxLength; 
    }
    
    
    public StringAttr(String name, String value, int maxLength) {
        super(name);
        this.value = value;
        this.maxLength = maxLength;
    }
    
    @Override
    public double calcDist(Attr other) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isConsistent(String v) {
        // TODO Auto-generated method stub
        if (value == null)
            return false;
        return v.equals(value);
    }

    @Override
    public void setValue(String v) {
    	if (this.maxLength >= 0 && v.length() > this.maxLength)
    		value = v.substring(0, this.maxLength);
    	else
    		value = v;
    }
    
    @Override 
    public String toString()
    {
        return value;
    }

	@Override
	public Attr clone() {
		return new StringAttr(name, value, maxLength);
	}
	
    @Override
    public Attr clone(boolean copyValues) {
        if (copyValues)
            return new StringAttr(name, value, maxLength);
        else
            return new StringAttr(name, maxLength);
    }
	

    @Override
    public boolean hasValue() {
        return value != null;
    }
        
    /**
     * No trivial solution for this attribute. Returns null.
     */
    @Override
    public TIntSet getValues() {
        return null;
    }



    @Override
    public String getStrValue(int value) {
        return name+":"+this.value;
    }
    
	@Override
	public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException {
		super.loadFromFile(in, globals);
        value = (String) in.readObject();
	}
 
	@Override
	public void saveToFile(ObjectOutputStream out) throws IOException {
		super.saveToFile(out);
		out.writeObject(value);
	}

	@Override
	public void storeGlobals(Map<String, Object> globals) {
		// no global variables
	}      

	@Override
	public void loadGlobals(Map<String, Object> globals) {
		// no global variables
	}      
	
    
}
