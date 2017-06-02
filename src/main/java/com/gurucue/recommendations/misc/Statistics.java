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
package com.gurucue.recommendations.misc;

import gnu.trove.iterator.TFloatIterator;
import gnu.trove.list.TFloatList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.hash.TFloatFloatHashMap;

public class Statistics {

    public static float average(TFloatList values)
    {
        if (values.size() == 0)
            return 0.0f;
        
        float avg = 0.0f;
        for (TFloatIterator fi = values.iterator(); fi.hasNext(); )
        {
            avg += fi.next();
        }
        avg /= values.size();
        return avg;
    }
    
    
    public static float std(TFloatList values)
    {
        if (values.size() <= 1)
            return 0.0f;
        
        float avg = Statistics.average(values);
        float std = 0.0f;
        
        for (TFloatIterator fi = values.iterator(); fi.hasNext(); )
        {
            std += Math.pow(fi.next() - avg, 2);
        }
        std /= (values.size() - 1);
        std = (float) Math.sqrt(std);
        return std;
    }
    
    // returns the cumulative normal distribution function (CNDF)
    // for a standard normal: N(0,1)
    // taken from http://stackoverflow.com/questions/442758/which-java-library-computes-the-cumulative-standard-normal-distribution-function
    public static double CNDF(double x)
    {
        int neg = (x < 0d) ? 1 : 0;
        if ( neg == 1) 
            x *= -1d;
        double k = (1d / ( 1d + 0.2316419 * x));
        double y = (((( 1.330274429 * k - 1.821255978) * k + 1.781477937) *
                       k - 0.356563782) * k + 0.319381530) * k;
        y = 1.0 - 0.398942280401 * Math.exp(-0.5 * x * x) * y;
        return (1d - neg) * y + neg * (1d - y);
    }    
    
    /**
     * Produces a list of ranks from an unordered list. Lowest value gets the lowest rank (0).
     * @param unlist
     * @return
     */
    public static TFloatArrayList rankData(TFloatArrayList unlist)
    {
        TFloatArrayList rankedList = new TFloatArrayList(unlist);
        // sort data
        unlist.sort();

        // create a hash for all values in list to produce correct ranks
        TFloatFloatHashMap s = new TFloatFloatHashMap();
        
        float key;
        int resp=0, ls = unlist.size();
        for (int i=0; i<ls; i+=resp)
        {
            resp = 1;
            key = unlist.get(i);
            while (i+resp < ls && unlist.get(i+resp) == key) resp++;
            s.put(key, i + ((float)resp-1)/2);
        }
        // rank data
        for (int i=0; i<rankedList.size(); i++)
        {
            key = rankedList.get(i);
            rankedList.set(i, s.get(key));
        }
        
        return rankedList;
    }
    
}
