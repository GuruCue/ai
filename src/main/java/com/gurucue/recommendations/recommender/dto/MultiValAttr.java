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

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class MultiValAttr extends Attr implements Cloneable {
    // values 
    int [] values;
    
    // hash from string values (as in DB) to int
    // a class that contains counter and map
    StringToIntMap map;
 
    // split string values?
    boolean split;
    
    public MultiValAttr(String name) {
        super(name);
        values = null;
        map = new StringToIntMap();
        split = false;
    }

    public MultiValAttr(String name, boolean split) {
        super(name);
        values = null;
        map = new StringToIntMap();
        this.split = split;
    }
    
    protected MultiValAttr(String name, boolean split, StringToIntMap map, int [] values) {
        super(name);
        this.values = values;
        this.map = map;
        this.split = split;
    }
    
    @Override
    public void setValue(String v)
    {
    	if (v == null || v.equalsIgnoreCase("null") || v.equalsIgnoreCase("--") ||  v.equalsIgnoreCase("__"))
    		return;
        if (split)
        {
        	String [] vals = v.split("[^a-zA-Z]+");
        	boolean first = true;
        	for (String vt : vals)
        	{
        		if (first)
        		{
        			first = false;
        			continue;
        		}
    	        if (map.containsKey(vt))
    	        	addValue(map.get(vt));
    	        else
    	        {
    	        	// temporary constraints: it should have at least 5 characters and it should begin with a large letter (like a name)
    	        	if (vt.length() >= 5 && Character.isUpperCase(vt.charAt(0)))
    	        	{
    	        		map.put(vt);
        	        	addValue(map.get(vt));
    	        	}
    	        }
        	}
        }
        else 
        {
	        if (!map.containsKey(v))
	            map.put(v);
        	addValue(map.get(v));
        }
    }
    
    private void addValue(int val)
    {
    	if (values == null)
    	{
    		values = new int[1];
    		values[0] = val;
    	}
    	else
    	{
    		final int l = values.length;
    		for (int i = 0; i < l; i++)
    			if (values[i] == val)
    				return;
    		values = Arrays.copyOf(values, l+1);
    		values[l] = val;
    	}
    }
    
    public void intersectValues(TIntSet allowedVal) {
    	if (values == null)
    		return;
    	// only keeps those values that are members of allowedVal set
    	TIntList tmp = new TIntArrayList();
    	final int l = values.length;
    	for (int i=0; i<l; i++)
    		if (!allowedVal.contains(values[i]))
    			tmp.add(values[i]);
    	values = tmp.toArray();
    }
    
    @Override 
    public String toString()
    {
        // optimized code below
        if (values == null)
            return "";
        final StringBuilder sb = new StringBuilder();
    	final int l = values.length;
    	for (int i=0; i<l; i++)
    	{
            sb.append(":");
            sb.append(map.inverseMap.get(values[i]));
    	}
        return sb.toString();
    }
    
    public int[] getValuesArray()
    {
        return values;
    }
    
    
    public double calcDist(Attr other) {

    	MultiValAttr o = (MultiValAttr) other;
    	if (values == null || o.values == null)
    		return 1.0;
    	
        if (o.values.length == 0 || this.values.length == 0)
            return 1.0;

        final int l = values.length;
        final int ol = o.values.length;
        
        int intersectionCount = 0;
        for (int i=0; i<l; i++)
        	for (int oi=0; oi<ol; oi++)
        		if (values[i] == o.values[oi])
        			intersectionCount ++;

        int unionCount = this.values.length + o.values.length - intersectionCount; // simple enough
        return 1 - intersectionCount/(double)unionCount;
    }
    
    @Override
    public boolean isConsistent(String v) {
        return containsValue(map.get(v));

    }
    
 	@Override
	public Attr clone() {
 	    if (values == null)
            return new MultiValAttr(name, split, map, null);
 	    else
 	        return new MultiValAttr(name, split, map, Arrays.copyOf(values, values.length));
	}
 	
 	
    @Override
    public Attr clone(boolean copyValues) {
    if (copyValues && values != null)
        return new MultiValAttr(name, split, map, Arrays.copyOf(values, values.length));
    else
        return new MultiValAttr(name, split, map, null);
    }


 	public boolean containsValue(int value)
 	{
 		if (values == null)
 			return false;
        final int l = values.length;
        for (int i=0; i<l; i++)
        	if (values[i] == value)
        		return true;
        return false;
 	}
 	
 	/**
 	 * This method returns the number of distinct possible values.
 	 * @return
 	 */
 	public int getNumberOfAllValues()
 	{
 	    return map.counter;   
 	}
 	
 	public int [] getAllValues()
 	{
        return map.map.values();
 	}
 	
    @Override
    public TIntSet getValues() {
    	if (values == null)
    		return null;
        return new TIntHashSet(values);
    }
 	
 	public String mapToString()
 	{
 	    String s = "";
 	    for (String k : map.map.keySet())
 	    {
 	       s += k + ":" + map.map.get(k) + "\n";    
 	    }
 	    return s;
 	}
 	
 	public String getKey(int value)
 	{
 	    return map.inverseMap.get(value);
 	}

    @Override
    public boolean hasValue() {
        if (values == null)
            return false;
        return (values.length > 0);
    };

    @Override
    public String getStrValue(int value) {
        return name+":"+getKey(value);
    }
    
	
	@Override
	public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException {
		super.loadFromFile(in, globals);
        values = (int []) in.readObject();
        split = in.readBoolean();
        map = (StringToIntMap) globals.get(name + "_map");
	}
 
	@Override
	public void saveToFile(ObjectOutputStream out) throws IOException {
		super.saveToFile(out);
		out.writeObject(values);
		out.writeBoolean(split);
	}

	@Override
	public void storeGlobals(Map<String, Object> globals) {
		globals.put(name + "_map", map);
	}  	    

	@Override
	public void loadGlobals(Map<String, Object> globals) {
        map = (StringToIntMap) globals.get(name + "_map");
	}  	    

}


// inner class for map definition
class StringToIntMap implements Serializable
{
	private static final long serialVersionUID = 5348147144119655083L;
	
	TObjectIntMap<String> map;
    TIntObjectMap<String> inverseMap;
    int counter;

    StringToIntMap()
    {
        counter=0;
        map = new TObjectIntHashMap<String>();
        inverseMap = new TIntObjectHashMap<String>();
    }
    
    boolean containsKey(String key)
    {
        return map.containsKey(key);
    }
    
    int get(String key)
    {
        return map.get(key);
    }
    
    void put(String key)
    {
        map.put(key, counter);
        inverseMap.put(counter, key);
        counter ++;
    }
    
    public String getKey(int value)
    {
        return inverseMap.get(value);
    }
}
