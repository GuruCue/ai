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
import gnu.trove.set.hash.TIntHashSet;

public class DiscAttr extends Attr implements Cloneable {
    
    public Integer value;
    
    public DiscAttr(String name) {
        super(name);
    }

    public DiscAttr(String name, Integer value) {
    	super(name);
    	this.value = value;
    }
    
    @Override
    public void setValue(String v) {
        if (v != null)
            value = Integer.valueOf(v);
    }
    
    @Override 
    public String toString()
    {
        return String.valueOf(value);
    }
    
    public double calcDist(Attr other) {
    	if (((DiscAttr) other).value.equals(value)) {
    		return 1.0;
    	} else {
    		return 0.0;
    	}
    }

    @Override
    public boolean isConsistent(String v) {
        if (value == null)
            return false;
        return Integer.valueOf(v).equals(value);
    }

    @Override
    public Attr clone() {
        return new DiscAttr(name, value);
    }

    
    @Override
    public Attr clone(boolean copyValues) {
        if (copyValues)
            return new DiscAttr(name, value);
        else
            return new DiscAttr(name);
    }
	

    @Override
    public boolean hasValue() {
        return null != value;
    }

    @Override
    public TIntSet getValues() {
        TIntSet t = new TIntHashSet();
        t.add(value);
        return t;
    }
    
    @Override
    public String getStrValue(int value) {
        return name+":"+this.value;
    }
    
	@Override
	public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException {
		super.loadFromFile(in, globals);
        value = (Integer) in.readObject();
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
