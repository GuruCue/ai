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

import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TObjectByteMap;
import gnu.trove.map.hash.TObjectByteHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class for storing meta data about events. 
 * The inner representation is an arraylist of objects, where each object is an array or an arraylist.
 * 
 */
public class ConsumerMetaEventsData implements Serializable {
	private static final long serialVersionUID = -8113585623334085846L;
    private final static Logger logger = LogManager.getLogger(ConsumerMetaEventsData.class.getName());
    
    ArrayList<Object> values; // list of meta values
    ArrayList<Object> maps; // maps that map original values to stored values (use when necessary)
    
    /**
     * Constructor used only in serialization
     */
    ConsumerMetaEventsData()
    {}
    
    /**
     * 
     * @param types
     */
    ConsumerMetaEventsData(String [] types)
    {
        values = new ArrayList<Object> ();
        maps = new ArrayList<Object> ();
        for (String t: types)
        {
            if (t.equalsIgnoreCase("long") || t.equalsIgnoreCase("date"))
            {
                TLongArrayList vals = new TLongArrayList();
                values.add(vals);
                maps.add(null);
            }
            else if (t.equalsIgnoreCase("int"))
            {
                TIntArrayList vals = new TIntArrayList();
                values.add(vals);
                maps.add(null);
            }
            else if (t.equalsIgnoreCase("byte"))
            {
                TByteArrayList vals = new TByteArrayList();
                values.add(vals);
                maps.add(null);
            }
            else if (t.equalsIgnoreCase("discrete"))
            {
                TByteArrayList vals = new TByteArrayList();
                values.add(vals);
                maps.add(new TObjectByteHashMap<String> ());
            }
        }
    }

    /**
     * Copy constructor.
     *
     * @param original
     * @param empty whether to create an empty copy, not copying any data
     */
    @SuppressWarnings("unchecked")
	public ConsumerMetaEventsData(final ConsumerMetaEventsData original, final boolean empty)
    {
        values = new ArrayList<Object>(original.values.size());
        for (final Object o : original.values)
        {
            if (o instanceof TLongArrayList)
            {
                values.add(empty ? new TLongArrayList() : new TLongArrayList((TLongArrayList)o));
            }
            else if (o instanceof TIntArrayList)
            {
                values.add(empty ? new TIntArrayList() : new TIntArrayList((TIntArrayList)o));
            }
            else if (o instanceof TByteArrayList)
            {
                values.add(empty ? new TByteArrayList() : new TByteArrayList((TByteArrayList)o));
            }
            else
            {
                final String reason = "Cannot make a deep instance copy: value element of unsupported type: " + o.getClass().getCanonicalName();
                logger.error(reason);
                throw new IllegalStateException(reason);
            }
        }
        
        maps = new ArrayList<Object>(original.maps.size());
        for (final Object o : original.maps)
        {
        	if (o == null)
        		maps.add(null);
        	else if (o instanceof TObjectIntHashMap<?>)
                maps.add(empty ? new TObjectByteHashMap<String>() : new TObjectByteHashMap<String> ((TObjectByteHashMap<String>)o));
            else
            {
                final String reason = "Cannot make a deep instance copy: value element of unsupported type: " + o.getClass().getCanonicalName();
                logger.error(reason);
                throw new IllegalStateException(reason);
            }        	
        }
    }

    @SuppressWarnings("unchecked")
	void addValue(Object result, String desc, int index, EventsDataDescriptor descriptor)
    {
        if (desc.equalsIgnoreCase("date"))
        {
            addDate(result, index, descriptor);
        }
        if (desc.equalsIgnoreCase("byte"))
        {
            addByte(result, index);
        }
        if (desc.equalsIgnoreCase("int"))
        {
            addInt(result, index);
        }
        if (desc.equalsIgnoreCase("discrete"))
        {
            final TByteList vals = (TByteList) values.get(index);
            final TObjectByteMap<String> map = (TObjectByteMap<String>) maps.get(index);
            String strVal = (String) result;
            if (!map.containsKey(strVal))
            {
            	map.put(strVal, (byte) (map.size() % 256));
            }
            byte val = map.get(strVal);
        	vals.add(val);
        }        
    }
    
    void addDate(Object result, int index, EventsDataDescriptor descriptor)
    {
        long date = ((Date)result).getTime() / 1000;
        descriptor.last_read_date = Math.max(date, descriptor.last_read_date); // updates last read date
        final TLongArrayList vals = (TLongArrayList) values.get(index);
        vals.add(date);
    }
    
    /**
     * Adds a byte value. Original value should be a string.
     * @param result
     * @param index
     */
    void addByte(Object result, int index)
    {
        byte bval;
        if (result instanceof Byte)
            bval = (Byte) result;
        else
        {
            String val = (String)result;
            try {
                bval = Byte.valueOf(val);
            }
            catch (NumberFormatException nfe) { 
                logger.warn("Failed to convert value " + val + " to a byte value");
                bval = 0;
            }
        }
        
        final TByteArrayList vals = (TByteArrayList) values.get(index);
        vals.add(bval);
    }
    
    /**
     * Adds an int value. Original value should be a string.
     * @param result
     * @param index
     */
    void addInt(Object result, int index)
    {
        int ival;
        if (result instanceof Integer)
            ival = (Integer) result;
        else
        {
            String val = (String)result;
            val = val.trim();
            try {
                ival = Integer.parseInt(val);
            }
            catch (NumberFormatException nfe) { 
                logger.warn("Failed to convert value " + val + " to an int value");
                ival = 0;
            }
        }
        
        final TIntArrayList vals = (TIntArrayList) values.get(index);
        vals.add(ival);
    }
    
    public long  [] getLongValuesArray(int metaIndex)
    {
        final TLongArrayList vals = (TLongArrayList) values.get(metaIndex);
        return vals.toArray();
    }

    public byte  [] getByteValuesArray(int metaIndex)
    {
        final TByteArrayList vals = (TByteArrayList) values.get(metaIndex);
        return vals.toArray();
    }

    public int  [] getIntValuesArray(int metaIndex)
    {
        final TIntArrayList vals = (TIntArrayList) values.get(metaIndex);
        return vals.toArray();
    }

    
    public long getLongValue(int metaIndex, int ratingIndex)
    {
        final TLongArrayList vals = (TLongArrayList) values.get(metaIndex);
        return vals.get(ratingIndex);
    }

    public int getIntValue(int metaIndex, int ratingIndex)
    {
        final TIntArrayList vals = (TIntArrayList) values.get(metaIndex);
        return vals.get(ratingIndex);
    }
    
    /**
     * Use this method when all events are read and, if possible, decrease the amount of memory spent. 
     * @param n
     */
    void finalizeReadings(int n)
    {
        
    }
    
    /**
     * Add values of another event. It is assumed that maps are identical.
     * 
     * @param original
     * @param eventIndex
     */
    
    public void addValuesOfEvent(final ConsumerMetaEventsData original, int eventIndex)
    {
        for (int i = values.size() - 1; i >= 0; i--)
        {
            final Object dst = values.get(i);
            final Object src = original.values.get(i);
            if (dst instanceof TLongArrayList)
            {
                ((TLongArrayList) dst).add(((TLongArrayList) src).get(eventIndex));
            }
            else if (dst instanceof TIntArrayList)
            {
                ((TIntArrayList) dst).add(((TIntArrayList) src).get(eventIndex));
            }
            else if (dst instanceof TByteArrayList)
            {
                ((TByteArrayList) dst).add(((TByteArrayList) src).get(eventIndex));
            }
        }
    }
    
    /**
     * Removes all but last number_keep_events meta of events
     * @param number_keep_events
     */
    public void removeData(int number_keep_events)
    {
        for (final Object o : values)
        {
            if (o instanceof TLongArrayList)
            {
                TLongArrayList ta = (TLongArrayList)o;
                if (ta.size() > number_keep_events)
                    ta.remove(0, ta.size()-number_keep_events);
            }
            else if (o instanceof TIntArrayList)
            {
                TIntArrayList ta = (TIntArrayList)o;
                if (ta.size() > number_keep_events)
                    ta.remove(0, ta.size()-number_keep_events);
            }
            else if (o instanceof TByteArrayList)
            {
                TByteArrayList ta = (TByteArrayList)o;
                if (ta.size() > number_keep_events)
                    ta.remove(0, ta.size()-number_keep_events);
            }
            else
            {
                final String reason = "Cannot remove data from a meta value of unsupported type: " + o.getClass().getCanonicalName();
                logger.error(reason);
                throw new IllegalStateException(reason);
            }
        }        
    }
    
    public void removeData()
    {
    	removeData(0);
    }
    
    public void removeDataIndices(TIntSet remove) {
        for (final Object o : values)
        {
            if (o instanceof TLongArrayList)
            {
                TLongArrayList ta = (TLongArrayList)o;
                
                for (int i = ta.size()-1; i>=0; i--)
                    if (remove.contains(i))
                        ta.removeAt(i);
            }
            else if (o instanceof TIntArrayList)
            {
                TIntArrayList ta = (TIntArrayList)o;
                for (int i = ta.size()-1; i>=0; i--)
                    if (remove.contains(i))
                        ta.removeAt(i);
            }
            else if (o instanceof TByteArrayList)
            {
                TByteArrayList ta = (TByteArrayList)o;
                for (int i = ta.size()-1; i>=0; i--)
                    if (remove.contains(i))
                        ta.removeAt(i);
            }
            else
            {
                final String reason = "Cannot remove data from a meta value of unsupported type: " + o.getClass().getCanonicalName();
                logger.error(reason);
                throw new IllegalStateException(reason);
            }
        }        
        
    }

    /**
     * Concatenates values. Merges maps. 
     * 
     * @param meta
     */
    @SuppressWarnings("unchecked")
	public void concatenate(ConsumerMetaEventsData meta) {
        final int valuesSize = values.size();
        for (int i = 0; i < valuesSize; i++) 
        {
        	// Values
            Object o = values.get(i);
            Object newo = meta.values.get(i);
            
            if (o instanceof TLongArrayList)
            {
                TLongArrayList ta = (TLongArrayList)o;
                TLongArrayList newta = (TLongArrayList)newo;
                ta.addAll(newta);
            }
            else if (o instanceof TIntArrayList)
            {
                TIntArrayList ta = (TIntArrayList)o;
                TIntArrayList newta = (TIntArrayList)newo;
                ta.addAll(newta);
            }
            else if (o instanceof TByteArrayList)
            {
                TByteArrayList ta = (TByteArrayList)o;
                TByteArrayList newta = (TByteArrayList)newo;
                ta.addAll(newta);
            }
            else
            {
                final String reason = "Cannot concatenate data from a meta value of unsupported type: " + o.getClass().getCanonicalName();
                logger.error(reason);
                throw new IllegalStateException(reason);
            }
            
            // Maps
            o = maps.get(i);
            newo = meta.maps.get(i);
            if (newo == null)
            	continue;
            else if (o == null)
            {
            	maps.set(i, newo);
            }
            else if (o instanceof TObjectIntHashMap<?>)
            {
            	// merge maps
            	((TObjectIntHashMap<String>) o).putAll((TObjectIntHashMap<String>) newo);
            }
            {
                final String reason = "Cannot merge maps from a meta value of unsupported type: " + o.getClass().getCanonicalName();
                logger.error(reason);
                throw new IllegalStateException(reason);
            }
        }
    }
}
