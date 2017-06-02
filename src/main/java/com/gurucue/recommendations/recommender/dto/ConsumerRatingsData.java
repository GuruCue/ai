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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gnu.trove.list.TByteList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Class contains ratings from a single user.
 */
public class ConsumerRatingsData extends ConsumerEventsData implements Serializable {
	private static final long serialVersionUID = 6935963047288179630L;
    private static final Logger logger = LogManager.getLogger(ConsumerRatingsData.class);
    private static final int[] EMPTY_INTS = new int[0];
    private static final byte[] EMPTY_BYTES = new byte[0];
    private final static byte MAXRATING = 100;
    private final static byte MINRATING = 0;
    public final static int INCREASE_ARRAY_STEP = 1000;
    
    public float sum; // sum of all ratings
    public int [] indices;
    public byte [] ratings;

    public ConsumerRatingsData()
    {
        this((String[])null);
    }
    
    public ConsumerRatingsData(String [] metaDesc)
    {
        n=0; sum=0;
        indices = EMPTY_INTS;
        ratings = EMPTY_BYTES;
        meta = new ConsumerMetaEventsData(metaDesc);
    }

    /**
     * Copy constructor.
     *
     * @param original
     * @param empty whether to create an empty copy, not copying any data
     */
    public ConsumerRatingsData(final ConsumerRatingsData original, final boolean empty) {
        meta = new ConsumerMetaEventsData(original.meta, empty);
        if (empty)
        {
            n = 0;
            sum = 0;
            indices = EMPTY_INTS;
            ratings = EMPTY_BYTES;
        }
        else
        {
            n = original.n;
            sum = original.sum;
            indices = Arrays.copyOf(original.indices, original.indices.length);
            ratings = Arrays.copyOf(original.ratings, original.ratings.length);
        }
    }
    
    @Override
    public void finalizeReading() {
        ratings = Arrays.copyOf(ratings, n);
        indices = Arrays.copyOf(indices, n);
    }

    public void addEvent(int productIndex, byte rating) {
        if (ratings.length == n)
        {
            ratings = Arrays.copyOf(ratings, n+INCREASE_ARRAY_STEP);
            indices = Arrays.copyOf(indices, n+INCREASE_ARRAY_STEP);
        }
        ratings[n] = rating;
        indices[n] = productIndex;
        n++;
        sum+=rating;
    }

    @Override
    public void addEvent(final EventsDataDescriptor descriptor, final Object[] result) {
        // first value is consumerID
        // second value is productID
    	if (result[1] == null || result[2] == null)
    		return;
        final long productId = (Long)result[1];
        final int productIndex = descriptor.dataStore.productIDs.get(productId);
        if (productIndex < 0) // doesn't exist
            return;
        final String[] metaDesc = descriptor.meta;
        // read rating and convert it to byte (memory optimization)
        final String sRating = (String)result[2];
        double drating;
        try {
            drating = Double.valueOf(sRating);
        }
        catch (NumberFormatException nfe) { // this should not happen, but anyway... if not readable, then set the default rating = 0
            logger.warn("Failed to convert rating " + sRating + " to a decimal number (product=" + productId + ", consumer=" + (Long)result[0] + ")");
            drating = 0;
        }
        // use exp4j to translate it to byte
        byte rating =descriptor.doubleToByte(drating);
        if (rating > MAXRATING)
            rating = MAXRATING;
        if (rating < MINRATING)
            rating = MINRATING;

        if (rating > descriptor.max)
            descriptor.max = rating;
        if (rating < descriptor.min)
            descriptor.min = rating;
        addEvent(productIndex, rating);
        for (int i = 3; i<result.length; i++)
            addMeta(result[i], metaDesc[i-3], i-3, descriptor);
    }

    @Override
    public String toString() {
        return "indices: " + Arrays.toString(indices) + ", ratings: " + Arrays.toString(ratings); 
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
        return new ConsumerRatingsData(this, empty);
    }

    @Override
    public void setFilteredData(final ConsumerEventsData eventsData, final long dateTrain, final boolean train) {
        final ConsumerRatingsData comparesData = (ConsumerRatingsData) eventsData;
        final long [] dates = comparesData.getMeta().getLongValuesArray(0);
        for (int e=0; e<comparesData.indices.length; e++)
        {
            if (dates[e] <= dateTrain && train || dates[e] > dateTrain && !train)
            {
                addEvent(comparesData.indices[e], comparesData.ratings[e]);
                meta.addValuesOfEvent(comparesData.getMeta(), e);
            }
        }
        finalizeReading();
    }

    @Override
    public void removeData(int numberKeepEvents) {
        indices = Arrays.copyOfRange(indices, Math.max(0, indices.length-numberKeepEvents), indices.length);
        ratings = Arrays.copyOfRange(ratings, Math.max(0, ratings.length-numberKeepEvents), ratings.length);
        n = indices.length;
        meta.removeData(numberKeepEvents);
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
        
        TIntList tmpIndices = new TIntArrayList();
        TByteList tmpRatings = new TByteArrayList();
        for (int i = 0; i < n; i++)
            if (!remove.contains(i))
            {
                tmpIndices.add(indices[i]);
                tmpRatings.add(ratings[i]);
            }
        indices = tmpIndices.toArray();
        ratings = tmpRatings.toArray();
        n = indices.length;
        meta.removeDataIndices(remove);
    }        

    @Override
    public void concatenate(ConsumerEventsData altData) {
        ConsumerRatingsData altRatingData = (ConsumerRatingsData) altData;
        ratings = Arrays.copyOf(ratings, n+altRatingData.n);
        indices = Arrays.copyOf(indices, n+altRatingData.n);
        System.arraycopy(altRatingData.ratings, 0, ratings, n, altRatingData.n);
        System.arraycopy(altRatingData.indices, 0, indices, n, altRatingData.n);
        n = indices.length;
        meta.concatenate(altData.meta);
    }
}
