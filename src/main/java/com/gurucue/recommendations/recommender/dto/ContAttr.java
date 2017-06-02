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
import java.util.Calendar;
import java.util.Map;

public class ContAttr extends Attr implements Cloneable {
    
    public Integer value;

    public ContAttr(String name) {
        super(name);
    }

    public ContAttr(String name, Integer value) {
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
    	ContAttr o = (ContAttr) other;
    	if (value == null || o.value == null) return 1.0;	// sasha: perhaps average is better?
    	
    	if (name.equals("run-time")) {
    		// this would probably be best if discretized
    		// e.g. 20min series, 40min series, short film, film, long film, (very long film)
    		// currently implemented distance:
    		// linear difference in minutes with cutoff at 90min (>= 90min equals max distance of 1)
    		return  Math.min(Math.abs(o.value - value) / 90.0, 1.0);
    	} else if (name.equals("production-year")) {
    		// this again might work best if discretized
    		// new film, not-yet-old film, older film, old film, (classic film)
    		// this attribute is harder to discretize it seems
    		// currently implemented distance:
    		// linear + added multiplier that is highest for newest films and 1.0 for old films
    		// bounded: max difference is 50yrs
    		int newest = Math.max(value, o.value);	// holds year of most recent film of the two
    		Calendar cal = Calendar.getInstance();
    		int cyear = cal.get(Calendar.YEAR);
    		double diff;

    		if (Math.abs(cyear-newest) <= 1) {
    			// at least one is current film
    			diff = Math.abs(value - o.value) * 5;
    		} else if (Math.abs(cyear-newest) <= 5) {
    			diff = Math.abs(value - o.value) * 2;
    		} else if (Math.abs(cyear-newest) <= 10) {
    			diff = Math.abs(value - o.value) * 1.5;
    		} else {
    			diff = Math.abs(value - o.value);
    		}
    		return Math.min(diff / 50.0, 1.0);
    	}
    	return 1.0;		// so that eclipse doesn't bother me
    }
    
    public boolean isConsistent(String v) 
    {
        if (value == null)
            return false;
        return value.equals(Integer.valueOf(v));
    }

    @Override
    public Attr clone() {
        return new ContAttr(name, value);
    }
    
    @Override
	public Attr clone(boolean copyValues) {
	    if (copyValues)
	        return new ContAttr(name, value);
	    else
            return new ContAttr(name);
	}

    @Override
    public boolean hasValue() {
        return null != value;
    }

    @Override
    public TIntSet getValues() {
        TIntSet t = new TIntHashSet();
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
