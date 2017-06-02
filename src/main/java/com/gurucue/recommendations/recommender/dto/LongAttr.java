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

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A class for representing Long values of attributes. 
 * 
 * Comment: the representation of attributes (as classes) should be reprogrammed, as they take way too much space. A good alternative might be 
 *
 */
public class LongAttr extends Attr implements Cloneable {
    private static Logger logger = LogManager.getLogger(LongAttr.class.getName());
    
    public Long value;

    public LongAttr(String name) {
        super(name);
    }

    public LongAttr(String name, Long value) {
        super(name);
        this.value = value;
    }

    @Override
    public void setValue(String v) {
        if (v != null)
        {
            try {
                value = Long.valueOf(v);
            } catch (NumberFormatException e) {
                value = null;
            }
        }
    }
    
    @Override 
    public String toString()
    {
        return String.valueOf(value);
    }
    
    public double calcDist(Attr other) {
        return 0;
    }
    
    public boolean isConsistent(String v) 
    {
        if (value == null)
            return false;
        return value.equals(Long.valueOf(v));
    }

	@Override
	public Attr clone() {
		return new LongAttr(name, value);
	}
	
    @Override
    public Attr clone(boolean copyValues) {
        if (copyValues)
            return new LongAttr(name, value);
        else
            return new LongAttr(name);
    }
	

    @Override
    public boolean hasValue() {
        return value != null;
    }
    
    /**
     * Warning: long values are simply cast to integer. If values are too big, then
     * make a new class, inherit LongAttr and reimplement getValues().
     */
    @Override
    public TIntSet getValues() {
        TIntSet t = new TIntHashSet();
        if (value != null)
            t.add(value.intValue());
        return t;
    }
    
    @Override
    public String getStrValue(int value) {
        return name+":"+this.value;
    }
    
	@Override
	public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException {
		super.loadFromFile(in, globals);
        value = (Long) in.readObject();
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
