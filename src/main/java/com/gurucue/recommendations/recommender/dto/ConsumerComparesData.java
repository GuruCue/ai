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
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Class contains compares of products by a single user.
 */
public class ConsumerComparesData extends ConsumerEventsData implements Serializable {
	private static final long serialVersionUID = 4046983430809790489L;
	private static final int[] EMPTY_INTS = new int[0];
    private static final byte[] EMPTY_BYTES = new byte[0];
    public static final int INCREASE_ARRAY_STEP = 1000;
    
    public int [] firstProducts; 
    public int [] secondProducts;
    public byte [] compares; // 0 - equal or don't know; 1: first is better; -1: second is better

    public ConsumerComparesData()
    {
        this((String[])null);
    }
    
    public ConsumerComparesData(String [] metaDesc)
    {
        n=0;
        firstProducts = EMPTY_INTS;
        secondProducts = EMPTY_INTS;
        compares = EMPTY_BYTES;
        meta = new ConsumerMetaEventsData(metaDesc);
    }

    /**
     * Copy constructor.
     *
     * @param original
     * @param empty whether to create an empty copy, not copying any data
     */
    public ConsumerComparesData(final ConsumerComparesData original, final boolean empty)
    {
        meta = new ConsumerMetaEventsData(original.meta, empty);
        if (empty)
        {
            n = 0;
            firstProducts = EMPTY_INTS;
            secondProducts = EMPTY_INTS;
            compares = EMPTY_BYTES;
        }
        else
        {
            n = original.n;
            firstProducts = Arrays.copyOf(original.firstProducts, original.firstProducts.length);
            secondProducts = Arrays.copyOf(original.secondProducts, original.secondProducts.length);
            compares = Arrays.copyOf(original.compares, original.compares.length);
        }
    }

    @Override
    public void finalizeReading() {
        firstProducts = Arrays.copyOf(firstProducts, n);
        secondProducts = Arrays.copyOf(secondProducts, n);
        compares = Arrays.copyOf(compares, n);
    }
    
    public void addEvent(int leftIndex, int rightIndex, byte compare) {
        if (firstProducts.length == n)
        {
            firstProducts = Arrays.copyOf(firstProducts, n+INCREASE_ARRAY_STEP);
            secondProducts = Arrays.copyOf(secondProducts, n+INCREASE_ARRAY_STEP);
            compares = Arrays.copyOf(compares, n+INCREASE_ARRAY_STEP);
        }
        firstProducts[n] = leftIndex;
        secondProducts[n] = rightIndex;
        compares[n] = compare;
        n++;
    }

    @Override
    public void addEvent(final EventsDataDescriptor descriptor, final Object[] result) {
        // first value is consumerID
        // second value is firstproductID
        // third value is secondproductID
        // fourth value ID of the selected product
        final long firstProductId = (Long)result[1];
        final long secondProductId = (Long)result[2];
        final Long selected = (Long)result[3];
        final int firstProductIndex = descriptor.dataStore.productIDs.get(firstProductId);
        final int secondProductIndex = descriptor.dataStore.productIDs.get(secondProductId);

        if ((firstProductIndex < 0) || (secondProductIndex < 0)) // one or both products don't exist
            return;
        final String[] metaDesc = descriptor.meta;
        final byte bselected;
        // better product was not selected
        if (selected == null)
            bselected = 0;
            // first product was selected as the better one
        else if (selected.longValue() == firstProductId)
            bselected = 1;
            // second product was selected as the better one
        else if (selected.longValue() == secondProductId)
            bselected = -1;
        else
            bselected = 0;

        addEvent(firstProductIndex, secondProductIndex, bselected);
        for (int i = 4; i<result.length; i++)
            addMeta(result[i], metaDesc[i-4], i-4, descriptor);
    }

    @Override
    public String toString() {
        return "first: " + Arrays.toString(firstProducts) + ", second: " + Arrays.toString(secondProducts) + ", compares: " + Arrays.toString(compares); 
    }

    @Override
    public boolean containsProduct(int productIndex) {
        for (int i=0; i<n; i++)
            if (firstProducts[i] == productIndex || secondProducts[i] == productIndex)
                return true;
        return false;
    }

    @Override
    public int[] getProductIndices() {
        int [] result = Arrays.copyOf(firstProducts, firstProducts.length + secondProducts.length);
        System.arraycopy(secondProducts, 0, result, firstProducts.length, secondProducts.length);
        return result;
    }

    @Override
    public void setProductIndices(int [] indices)
    {
        this.firstProducts = Arrays.copyOf(indices, firstProducts.length);
        this.secondProducts = Arrays.copyOfRange(indices, firstProducts.length, firstProducts.length + secondProducts.length);
    }
    
    @Override
    public ConsumerEventsData createCopy(final boolean empty) {
        return new ConsumerComparesData(this, empty);
    }

    @Override
    public void setFilteredData(final ConsumerEventsData eventsData, final long dateTrain, final boolean train) {
        final ConsumerComparesData comparesData = (ConsumerComparesData) eventsData;
        final long [] dates = comparesData.getMeta().getLongValuesArray(0);
        for (int e=0; e<comparesData.firstProducts.length; e++)
        {
            if (dates[e] <= dateTrain && train || dates[e] > dateTrain && !train)
            {
                addEvent(comparesData.firstProducts[e], comparesData.secondProducts[e], comparesData.compares[e]);
                meta.addValuesOfEvent(comparesData.getMeta(), e);
            }
        }
        finalizeReading();
    }
    
    @Override
    public void removeData(int numberKeepEvents) {
        firstProducts = Arrays.copyOfRange(firstProducts, Math.max(0, firstProducts.length-numberKeepEvents), firstProducts.length);
        secondProducts = Arrays.copyOfRange(secondProducts, Math.max(0, secondProducts.length-numberKeepEvents), secondProducts.length);
        compares = Arrays.copyOfRange(compares, Math.max(0, compares.length-numberKeepEvents), compares.length);
        n = firstProducts.length;
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
        
        TIntList tmpFirst = new TIntArrayList();
        TIntList tmpSecond = new TIntArrayList();
        TByteList tmpCompares = new TByteArrayList();
        for (int i = 0; i < n; i++)
            if (!remove.contains(i))
            {
                tmpFirst.add(firstProducts[i]);
                tmpSecond.add(secondProducts[i]);
                tmpCompares.add(compares[i]);
            }
        firstProducts = tmpFirst.toArray();
        secondProducts = tmpSecond.toArray();
        compares = tmpCompares.toArray();
        n = firstProducts.length;
        meta.removeDataIndices(remove);
    }    

    @Override
    public void concatenate(ConsumerEventsData altData) {
        ConsumerComparesData altRatingData = (ConsumerComparesData) altData;
        firstProducts = Arrays.copyOf(firstProducts, n+altRatingData.n);
        secondProducts = Arrays.copyOf(secondProducts, n+altRatingData.n);
        System.arraycopy(altRatingData.firstProducts, 0, firstProducts, n, altRatingData.n);
        System.arraycopy(altRatingData.secondProducts, 0, secondProducts, n, altRatingData.n);
        n = firstProducts.length;
        meta.concatenate(altData.meta);
    }    
}
