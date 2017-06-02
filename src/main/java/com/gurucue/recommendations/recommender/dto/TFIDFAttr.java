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

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

/**
 * A class that contains a multivalued attribute (similar as MulitValAttr), however it selects only N most informative attribute values. 
 * The imporatance of an attribute value is determined by the TF-IDF measure. 
 * 
 * TODO: lemmatization or stemming should be added to improve robustness. 
 *
 */
public class TFIDFAttr extends Attr implements Cloneable {
	int [] selected_values; // all values 
	TIntIntMap valuesCounter;
	// number of important features
	int nfeatures;
	// number of ngrams to extract
	int ngrams;
	// should ony titles be stored?
	boolean onlyTitles;
	// a class that contains frequencies of string values in documents
	InverseDocumentFrequency freq;
    // hash from string values (as in DB) to int
    // a class that contains counter and map
    StringToIntMap map;
	
	
    public TFIDFAttr(String name) {
        super(name);
        selected_values = null;
        valuesCounter = null;
        map = new StringToIntMap();
        freq = new InverseDocumentFrequency();
        ngrams = 1;
        nfeatures = 1;
        onlyTitles = true;
    }

    public TFIDFAttr(String name, int ngrams, int nfeatures, boolean onlyTitles) {
        super(name);
        selected_values = null;
        valuesCounter = null;
        map = new StringToIntMap();
        freq = new InverseDocumentFrequency();
        this.ngrams = ngrams;
        this.nfeatures = nfeatures;
        this.onlyTitles = true;
    }

    private TFIDFAttr(String name, int ngrams, int nfeatures, boolean onlyTitles, StringToIntMap map, InverseDocumentFrequency freq) {
    	super(name);
        this.map = map;
        this.freq = freq;
        this.ngrams = ngrams;
        this.nfeatures = nfeatures;
        selected_values = null;
        this.onlyTitles = true;
    }
    
    private TFIDFAttr(String name, int ngrams, int nfeatures, boolean onlyTitles, StringToIntMap map, InverseDocumentFrequency freq, int [] selected_values) {
    	super(name);
        this.selected_values = selected_values;
        this.freq = freq;
        this.map = map;
        this.ngrams = ngrams;
        this.nfeatures = nfeatures;
        this.onlyTitles = true;
    }
    
    @Override
    public void setValue(String v)
    {
    	// if values were already determined, adding is not possible
    	if (selected_values != null)
    		return; 
    	int hsh = v.hashCode();
    	
    	if (v == null || v.equalsIgnoreCase("null"))
    		return;
        if (valuesCounter == null)
        {
        	valuesCounter = new TIntIntHashMap(0, 0.9f);
        	freq.increaseN(hsh);
        }

        String [] vals = v.split("[^a-zA-Z]+");
        for (int vi = 0; vi < vals.length; vi ++)
    	{
        	String val = "";
        	for (int gi = 0; gi < ngrams; gi ++)
        	{
        		if (vals[vi+gi].length() < 4)
        			continue;
        		if (vi + gi < vals.length && (this.onlyTitles == false || Character.isUpperCase(vals[vi+gi].charAt(0))))
        			val += vals[vi+gi].toLowerCase();
        		else 
        			continue;
				if (!map.containsKey(val))
					map.put(val);
		        final int key = map.get(val);
				if (valuesCounter.get(key) < 1)
				{
		            freq.put(hsh, key);
		        }
				valuesCounter.put(key, valuesCounter.get(key) + 1);
        	}
        }
        freq.puthash(hsh);
    }
    
    public void intersectValues(TIntSet allowedVal) {
    	if (selected_values == null)
    		return;
    	// only keeps those values that are members of allowedVal set
    	TIntList tmp = new TIntArrayList();
    	final int l = selected_values.length;
    	for (int i=0; i<l; i++)
    		if (!allowedVal.contains(selected_values[i]))
    			tmp.add(selected_values[i]);
    	selected_values = tmp.toArray();
    }
    
    @Override 
    public String toString()
    {
        // optimized code below
        if (selected_values == null)
            return "";
        final StringBuilder sb = new StringBuilder();
    	final int l = selected_values.length;
    	for (int i=0; i<l; i++)
    	{
            sb.append(":");
            sb.append(map.inverseMap.get(selected_values[i]));
    	}
        return sb.toString();
    }
    
    public int[] getValuesArray()
    {
        return selected_values;
    }
    
    public double calcDist(Attr other) {

    	TFIDFAttr o = (TFIDFAttr) other;
        if (o.selected_values == null || this.selected_values == null)
            return 1.0;
        
        if (o.selected_values.length == 0 || this.selected_values.length == 0)
            return 1.0;

        final int l = selected_values.length;
        final int ol = o.selected_values.length;
        
        int intersectionCount = 0;
        for (int i=0; i<l; i++)
        	for (int oi=0; oi<ol; oi++)
        		if (selected_values[i] == o.selected_values[oi])
        			intersectionCount ++;

        int unionCount = this.selected_values.length + o.selected_values.length - intersectionCount; // simple enough
        return 1 - intersectionCount/(double)unionCount;
    }

    @Override
    public boolean isConsistent(String v) {
        return containsValue(map.get(v));

    }
    
 	@Override
	public Attr clone() {
 	    if (selected_values == null)
            return new TFIDFAttr(name, ngrams, nfeatures, onlyTitles, map, freq);
 	    else
 	        return new TFIDFAttr(name, ngrams, nfeatures, onlyTitles, map, freq, Arrays.copyOf(selected_values, selected_values.length));
	}
 	
 	
    @Override
    public Attr clone(boolean copyValues) {
    if (copyValues && selected_values != null)
    	return new TFIDFAttr(name, ngrams, nfeatures, onlyTitles, map, freq, Arrays.copyOf(selected_values, selected_values.length));
    else
    	return new TFIDFAttr(name, ngrams, nfeatures, onlyTitles, map, freq);
    }


 	public boolean containsValue(int value)
 	{
        if (selected_values == null)
            return false;
 		
        final int l = selected_values.length;
        for (int i=0; i<l; i++)
        	if (selected_values[i] == value)
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
        if (selected_values == null)
        	return null;
        
        return new TIntHashSet(selected_values);
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
        if (selected_values == null)
            return false;
        return (selected_values.length > 0);
    };

    @Override
    public String getStrValue(int value) {
        return name+":"+getKey(value);
    }
    
    @Override
    public void postprocess() {
    	if (valuesCounter != null)
    	{
    		TIntDoubleMap scores = new TIntDoubleHashMap();
    		for (TIntIntIterator it = valuesCounter.iterator(); it.hasNext(); )
    		{
    			it.advance();
    			final int key = it.key();
    			final int count = it.value();
    			
    			final double tfidf = count * freq.getIDF(key);
    			scores.put(key, tfidf);
    		}
    		// select best scores
    		double threshold = 0;
    		if (scores.size() > nfeatures)
    		{
        		double [] values = scores.values();
        		Arrays.sort(values);
        		threshold = values[values.length - nfeatures];
    		}

    		TIntList tmp_values = new TIntArrayList();
    		
    		// set selected_values
    		for (TIntDoubleIterator it = scores.iterator(); it.hasNext(); )
    		{
    			it.advance();
    			final int key = it.key();
    			final double score = it.value();
    			
    			if (score > 0 && score >= threshold)
    				tmp_values.add(key);
    		}
    		
    		selected_values = tmp_values.toArray();
    		
    		// set values counter to null
    		valuesCounter = null;
    	}
    }
	
	@Override
	public void loadFromFile(ObjectInputStream in, Map<String, Object> globals) throws IOException, ClassNotFoundException {
		super.loadFromFile(in, globals);
		selected_values = (int []) in.readObject();
		ngrams = in.readInt();
		nfeatures = in.readInt();
        map = (StringToIntMap) globals.get(name + "_map");
        freq = (InverseDocumentFrequency) globals.get(name + "_freq");
	}
 
	@Override
	public void saveToFile(ObjectOutputStream out) throws IOException {
		super.saveToFile(out);
		out.writeObject(selected_values);
		out.writeInt(ngrams);
		out.writeInt(nfeatures);
	}

	@Override
	public void storeGlobals(Map<String, Object> globals) {
		globals.put(name + "_map", map);
		globals.put(name + "_freq", freq);
	}

	@Override
	public void loadGlobals(Map<String, Object> globals) {
        map = (StringToIntMap) globals.get(name + "_map");
        freq = (InverseDocumentFrequency) globals.get(name + "_freq");
	}
}

// inner class for inverse document frequency definition
class InverseDocumentFrequency implements Serializable
{
	private static final long serialVersionUID = 8012573683231443201L;
	
	TIntIntMap counter; // frequencies of words
	float N; // number of all documents containing at least one value
	TIntSet hashes; // contain hashes of documents to spot duplicates

	InverseDocumentFrequency()
    {
        N=0;
        counter = new TIntIntHashMap();
        hashes = new TIntHashSet();
    }

	double getIDF(int word)
	{
		final int cnt = counter.get(word);
		if (cnt < 5 || N < 1)
			return 0;
		return Math.log(N / counter.get(word));
	}

	void increaseN(int h)
	{
		if (hashes.contains(h))
			return;
		N++;
	}
	
    void put(TIntSet values)
    {
    	if (values.size() == 0)
    		return;
    	for (TIntIterator it = values.iterator(); it.hasNext(); )
    	{
    		final int val = it.next();
			counter.put(val, counter.get(val) + 1);
    	}
    }
    
    void put(int h, int val)
    {
		if (hashes.contains(h))
			return;
		counter.put(val, counter.get(val) + 1);
    }
    
    void puthash(int h)
    {
    	hashes.add(h);
    }
}

