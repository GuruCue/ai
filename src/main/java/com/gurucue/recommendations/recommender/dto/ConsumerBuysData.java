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

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class contains buys-events of products for a single user. 
 * This class in not necessary for bought products only, 
 * it could be also used for views, clicks or anything similar.
 */
public class ConsumerBuysData extends ConsumerEventsData implements Serializable {
	private static final long serialVersionUID = -5410022908754759201L;
    private static final int[] EMPTY_INDICES = new int[0];
    private static final Logger logger = LogManager.getLogger(ConsumerBuysData.class.getName());;

    
    public int [] indices;
    
    public ConsumerBuysData()
    {
        this((String[])null);
    }
    
    public ConsumerBuysData(String [] metaDesc)
    {
        n=0;
        indices = EMPTY_INDICES;
        meta = new ConsumerMetaEventsData(metaDesc);
    }

    /**
     * Copy constructor.
     *
     * @param original
     * @param empty whether to create an empty copy, not copying any data
     */
    public ConsumerBuysData(final ConsumerBuysData original, final boolean empty) {
        meta = new ConsumerMetaEventsData(original.meta, empty);
        if (empty)
        {
            n = 0;
            indices = EMPTY_INDICES;
        }
        else
        {
            n = original.n;
            indices = Arrays.copyOf(original.indices, original.indices.length);
        }
    }

    @Override
    public void finalizeReading() {
        indices = Arrays.copyOf(indices, n);
        meta.finalizeReadings(n);
        

        final long [] newDate = meta.getLongValuesArray(0);
        if (newDate.length > 0 && newDate.length != n)
        {
            logger.error("Error in reading data: number of dates does not correspond to number of items. ");
        }        
    }
    
    public void addEvent(int productIndex) {
        // if both product indices are already in, just replace it
        if (indices.length == n)
        {
            indices = Arrays.copyOf(indices, n+INCREASE_ARRAY_STEP);
        }
        indices[n] = productIndex;
        n++;
    }

    @Override
    public void addEvent(final EventsDataDescriptor descriptor, final Object [] result) {
        // first value is consumerID
        // second value is productID
        // third value, fourth value, etc are metas
    	final Long tmpProd = (Long)result[1];
    	if (tmpProd == null)
    		return;
        final int productIndex = descriptor.dataStore.productIDs.get(tmpProd);
        if (productIndex < 0) // doesn't exist
        {
            return;
        }
        final String[] metaDesc = descriptor.meta;

        addEvent(productIndex);
        
        for (int i = 2; i<2+metaDesc.length; i++)
            addMeta(result[i], metaDesc[i-2], i-2, descriptor);
    }
    
    @Override
    public String toString() {
        return "indices: " + Arrays.toString(indices); 
    }

    @Override
    public boolean containsProduct(int productIndex) {
        for (int i=0; i<n; i++)
            if (indices[i] == productIndex)
                return true;
        return false;
    }

    @Override
    public int[] getProductIndices() {
        return indices;
    }
    
    @Override
    public void setProductIndices(int [] indices)
    {
    	this.indices = indices;
    }

    @Override
    public ConsumerEventsData createCopy(final boolean empty) {
        return new ConsumerBuysData(this, empty);
    }

    @Override
    public void setFilteredData(final ConsumerEventsData eventsData, final long dateTrain, final boolean train) {
        final ConsumerBuysData buysData = (ConsumerBuysData) eventsData;
        final long [] dates = buysData.getMeta().getLongValuesArray(0);
        for (int e=0; e<buysData.indices.length; e++)
        {
            if (dates[e] <= dateTrain && train || dates[e] > dateTrain && !train)
            {
                addEvent(buysData.indices[e]);
                meta.addValuesOfEvent(buysData.getMeta(), e);
            }
        }
        finalizeReading();
    }
    
    @Override
    public void removeData(int numberKeepEvents) {
    	if (numberKeepEvents == 0)
    	{
    		indices = new int [0];
    		n = 0;
    		meta.removeData();
    	}
    	else 
    	{
    		indices = Arrays.copyOfRange(indices, Math.max(0, indices.length-numberKeepEvents), indices.length);
    		n = indices.length;
    		meta.removeData(numberKeepEvents);
    	}
    }

    @Override
    public void removeDataTime(EventsDataDescriptor descriptor) {
        final long last_allowed_date = descriptor.last_read_date - descriptor.keep_in_memory_time;
        final int date_id = descriptor.getMetaIndex("date");
        if (date_id < 0)
            return;
        final long [] dates = meta.getLongValuesArray(date_id);

        // iterate through dates, store indices are removed
        TIntSet remove = new TIntHashSet();
        for (int i = dates.length-1; i>=0; i--)
        {
            if (dates[i] < last_allowed_date)
                remove.add(i);
        }
        if (remove.size() == 0)
            return;
        
        TIntList tmp = new TIntArrayList();
        for (int i = 0; i < n; i++)
            if (!remove.contains(i))
                tmp.add(indices[i]);
        indices = tmp.toArray();
        n = indices.length;
        meta.removeDataIndices(remove);
    }
    
    @Override
    public void concatenate(ConsumerEventsData altData) {
        ConsumerBuysData altRatingData = (ConsumerBuysData) altData;
        indices = Arrays.copyOf(indices, n+altRatingData.n);
        System.arraycopy(altRatingData.indices, 0, indices, n, altRatingData.n);
        n = indices.length;
        meta.concatenate(altData.meta);
    }    
}
