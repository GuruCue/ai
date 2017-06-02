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

import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;


/**
 * A class for handling meta data and putting it into the right context. Meta data are discretized.
 * Contexts are numbered with integer values starting at 1. A "no context" context valued 0 is always returned.
 * 
 */
public class ContextDiscretizer implements Serializable {
    private static final long serialVersionUID = 1L;
    
    final String [] CONTEXTNAMES; // names of contexts used
    final int [] CONTEXTMETASINDICES; // index of meta data that the context refers to
    final int [] CONTEXT_START_NUMBER; // start index of a context
    final int [] CONTEXT_ATTRIBUTES; // start index of a context
    int numOfContexts;
    
    // values related to DAYTIME context
    int [] DAYTIME_HOURS;
    Calendar cal;
    
    public ContextDiscretizer (String name, Settings settings)
    {
        CONTEXTNAMES = settings.getAsStringArray(name + "_CONTEXT_NAMES");
        CONTEXTMETASINDICES = settings.getAsIntArray(name + "_CONTEXT_METAS_INDICES");
        CONTEXT_START_NUMBER = new int[CONTEXTNAMES.length];
        CONTEXT_ATTRIBUTES = settings.getAsIntArray(name + "_CONTEXT_ATTRIBUTES");
        numOfContexts = 1;
        cal = Calendar.getInstance();
        for (int i=0; i<CONTEXTNAMES.length; i++)
        {
            if (CONTEXTNAMES[i].equalsIgnoreCase("daytime"))
            {
                DAYTIME_HOURS = settings.getAsIntArray(name + "_CONTEXT_DAYTIME_HOURS");
                CONTEXT_START_NUMBER[i] = numOfContexts;
                numOfContexts += DAYTIME_HOURS.length;
            }
            else if (CONTEXTNAMES[i].equalsIgnoreCase("dayweekhour")) // per hour per day in week
            {
                CONTEXT_START_NUMBER[i] = numOfContexts;
                numOfContexts += dayweekhourn();
            }
        }
        
    }
    
    
    /**
     * Gets context positions from attributes and meta data.
     * @param meta
     * @param ratingIndex
     * @return
     */
    public TIntList getContext(DataStore data, int productIndex, ConsumerMetaEventsData meta, int ratingIndex)
    {
    	TIntList positions = new TIntArrayList();
        positions.add(0);
        for (int i=0; i<CONTEXTNAMES.length; i++)
        {
            if (CONTEXTNAMES[i].equalsIgnoreCase("daytime"))
            {
                long date;
                if (CONTEXT_ATTRIBUTES[i] == -1) // get value from meta
                    date = meta.getLongValue(CONTEXTMETASINDICES[i], ratingIndex);
                else
                {
                    Attr a = data.getAttr(productIndex, CONTEXT_ATTRIBUTES[i]);
                    if (null != a && a.hasValue())
                        date = ((LongAttr) a).value;
                    else
                        date = System.currentTimeMillis();
                }
                cal.setTime(new Date(date));
                final int hour = cal.get(Calendar.HOUR_OF_DAY);
                for (int j=0; j<DAYTIME_HOURS.length; j++)
                    if (hour <= DAYTIME_HOURS[j])
                    {
                        positions.add(CONTEXT_START_NUMBER[i]+j);
                        break;
                    }
            }
            else if (CONTEXTNAMES[i].equalsIgnoreCase("dayweekhour"))
            {
                long date = meta.getLongValue(CONTEXTMETASINDICES[i], ratingIndex);
                positions.add(dayweekhour(date));
            }
        }
        return positions;
    }
  
    
    /**
     * Gets context positions from attributes and date values.
     * @param data
     * @param productIndex
     * @return
     */
    public TIntList getContext(DataStore data, int productIndex, long date)
    {
    	TIntList positions = new TIntArrayList();
        positions.add(0);

        for (int i=0; i<CONTEXTNAMES.length; i++)
        {
            if (CONTEXTNAMES[i].equalsIgnoreCase("daytime"))
            {
                cal.setTime(new Date(date));
                final int hour = cal.get(Calendar.HOUR_OF_DAY);
                for (int j=0; j<DAYTIME_HOURS.length; j++)
                    if (hour <= DAYTIME_HOURS[j])
                    {
                        positions.add(CONTEXT_START_NUMBER[i]+j);
                        break;
                    }
            }
            else if (CONTEXTNAMES[i].equalsIgnoreCase("dayweekhour"))
            {
                positions.add(dayweekhour(date));
            }
        }
        return positions;
    }
    
    /**
     * Gets context positions from attributes and tags (provided when asking for recommendations).
     * 
     * @param data
     * @param product_index
     * @param tags
     * @return
     */
    public TIntList getContext(final DataStore data, final int productIndex, Map<String, String> tags)
    {
    	TIntList positions = new TIntArrayList();
        positions.add(0);

        for (int i=0; i<CONTEXTNAMES.length; i++)
        {
            if (CONTEXTNAMES[i].equalsIgnoreCase("daytime"))
            {
                long date;
                if (CONTEXT_ATTRIBUTES[i] == -1) // get value from tags
                    if (tags.containsKey(CONTEXTNAMES[i]))
                        date = Long.parseLong(tags.get(CONTEXTNAMES[i]));
                    else
                        date = System.currentTimeMillis();
                else // get value from an attribute
                {
                    Attr a = data.getAttr(productIndex, CONTEXT_ATTRIBUTES[i]);
                    if (null != a && a.hasValue())
                        date = ((LongAttr) a).value;
                    else
                        date = System.currentTimeMillis();
                }
                cal.setTime(new Date(date));
                final int hour = cal.get(Calendar.HOUR_OF_DAY);
                for (int j=0; j<DAYTIME_HOURS.length; j++)
                    if (hour <= DAYTIME_HOURS[j])
                    {
                        positions.add(CONTEXT_START_NUMBER[i]+j);
                        break;
                    }
            }
            else if (CONTEXTNAMES[i].equalsIgnoreCase("dayweekhour"))
            {
                long date;
                if (CONTEXT_ATTRIBUTES[i] == -1) // get value from tags
                    if (tags.containsKey(CONTEXTNAMES[i]))
                        date = Long.parseLong(tags.get(CONTEXTNAMES[i]));
                    else
                        date = System.currentTimeMillis();
                else // get value from an attribute
                {
                    Attr a = data.getAttr(productIndex, CONTEXT_ATTRIBUTES[i]);
                    if (null != a && a.hasValue())
                        date = ((LongAttr) a).value;
                    else
                        date = System.currentTimeMillis();
                }                
                positions.add(dayweekhour(date));
            }            
        }
        return positions;
    }
    
    public int numOfContexts() {
        return numOfContexts;
    }
    
    private int dayweekhour(long time)
    {
    	Calendar cal = Calendar.getInstance();
    	final Date tmp = new Date(time*1000);
    	
        cal.setTime(tmp);
        final int hour = cal.get(Calendar.HOUR_OF_DAY);
        final int day = cal.get(Calendar.DAY_OF_WEEK);
        int weekend = 0;
        if (day == Calendar.SATURDAY || day == Calendar.SUNDAY)
        	weekend = 1;
        return ((hour/4) + weekend * 6 + 1);
    }
    
    private int dayweekhourn()
    {
    	return 12;
    }
    
}