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

/**
 * Class for representing boolean attributes
 */
public class BooleanAttr extends Attr implements Cloneable { 
    
    public Boolean value;

    public BooleanAttr(String name) {
        super(name);
    }

    public BooleanAttr(String name, Boolean value) {
        super(name);
        this.value = value;
    }

    @Override
    public void setValue(String v) {
        if (v != null)
            value = Boolean.valueOf(v);
    }
    
    @Override 
    public String toString()
    {
        return String.valueOf(value);
    }
    
    public double calcDist(Attr other) {
        if (value != ((BooleanAttr)other).value)
            return 1.0;
        return 0.0;
    }
    
    public boolean isConsistent(String v) 
    {
        if (value == null)
            return false;
        return value.equals(Boolean.valueOf(v));
    }

    @Override
    public Attr clone() {
        return new BooleanAttr(name, value);
    }
    
    @Override
    public Attr clone(boolean copyValues) {
        if (copyValues)
            return new BooleanAttr(name, value);
        else
            return new BooleanAttr(name);
    }

    @Override
    public boolean hasValue() {
        return null != value;
    }

    @Override
    public TIntSet getValues() {
        TIntSet t = new TIntHashSet();
        t.add(value.booleanValue()?1:0);
        return t;
    }

    @Override
    public String getStrValue(int value) {
        return name+":"+(this.value?"true":"false");
    }

	@Override
	public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException {
		super.loadFromFile(in, globals);
		value = in.readBoolean();
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
