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

/**
 * Abstract class for representing attributes of products.
 */
 
public abstract class Attr {
    
    public String name;
    
    /**
     * Constructor
     * @param name Name of attribute
     */
    public Attr(String name)
    {
        this.name = name;
    }

    /**
     * Sets value of an attribute. 
     */
    abstract public void setValue(String v);
    
    /**
     * Calculates distance according to this attribute between this attribute's value 
     * and given attribute's value (other's value). 
     */
    abstract public double calcDist(Attr other);

    /**
     *  This method is used for filtering products. It returns True if the value of the attribute is consistent (usually it means the same) 
     *  with the provided value.
     *  
     * @param v Value used for filtering products.
     * @return
     */
    abstract public boolean isConsistent(String v);
    
    /**
     * Returns true if value of attribute has been set. 
     * @return
     */
    abstract public boolean hasValue();
    
    /**
     * Returns a set of values (discretized in the case of float variables). In most cases, this will return only a single value.
     * 
     * @return
     */
    abstract public TIntSet getValues();
    
    /**
     * String representation of a particular value 
     * @param value
     * @return
     */
    abstract public String getStrValue(int value);
    
    abstract public Attr clone();
    abstract public Attr clone(boolean copyValues);
    
    /**
     * If attribute contains any global variables that are the same for all values,
     * it should be stored in the globals map. These values then do not need to be saved in the saveToFile methods,
     * as DataStore will store them.
     * @param globals
     */
    abstract public void storeGlobals(Map<String, Object> globals);

    /**
     * @param globals
     */
    abstract public void loadGlobals(Map<String, Object> globals);
    
    /**
     * Saves values to a file. Global variables should not be stored, are already handled by setGlobals.
     */
    public void saveToFile(ObjectOutputStream out) throws IOException
    {
    	out.writeObject(name);
    }
    
    /**
     * Loads all relevant data either from file or from globals map.
     * @param in
     * @param globals
     */
    public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException 
    {
    	name = (String) in.readObject();
    }
    
    public void postprocess()
    {}
}
    
    
