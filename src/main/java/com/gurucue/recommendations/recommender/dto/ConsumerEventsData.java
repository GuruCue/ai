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

import java.io.Serializable;

/**
 * Abstract class for all ConsumerEvents data. 
 * At the moment it is not used.
 * 
 */
public abstract class ConsumerEventsData implements Serializable {
	private static final long serialVersionUID = 1134125560184756833L;
	public static final int INCREASE_ARRAY_STEP = 100;
    
    protected int n; // user involved in n events
    protected ConsumerMetaEventsData meta; // meta values about consumer events;
    
    /**
     * Call this after all information about this user was read (e.g. products ...)
     */
    public abstract void finalizeReading();
    /**
     * Returns true if the product is among the events of the user.
     * @param productIndex
     * @return
     */
    public abstract boolean containsProduct(int productIndex);
    
    /**
     * Returns all product indices relevant to the events of this consumer.
     * @return
     */
    public abstract int [] getProductIndices();
    
    /**
     * Use this method exclusively to replace old indices with new (when indices of products have changed).
     * Now, if a product was deleted, its index will be set to zero. These indices should be removed from the list(s) and also removed from the meta.
     * @param indices
     */
    public abstract void setProductIndices(int [] indices);

    /**
     * Returns number of events.
     * @return
     */
    public int getN()
    {
        return n;
    }
    
    /**
     * Adds meta to an event. Result is an Object retrieved from a reader. MetaDesc is a description of type of meta. Index is the index of meta attribute. 
     * Meta should always be added to the last event added in the data store. 
     * @param result
     * @param metaDesc
     * @param index
     * @param descriptor 
     */
    public void addMeta(Object result, String metaDesc, int index, EventsDataDescriptor descriptor)
    {
        meta.addValue(result, metaDesc, index, descriptor);
    }
    
    public ConsumerMetaEventsData getMeta()
    {
        return meta;
    }
    
    public abstract void removeData(final int number_keep_events);

    public abstract void addEvent(final EventsDataDescriptor descriptor, final Object [] result);

    /**
     * Clones the instance, without using Java's clone semantics.
     *
     * @param empty whether to create a structurally identical, but empty, copy
     * @return a deep copy of this instance
     */
    public abstract ConsumerEventsData createCopy(final boolean empty);

    /**
     * Creates a subset of events from events
     * @param eventsData Dull data on events.
     * @param dateTrain events before and on that moment are train data, others test data
     * @param train (train = true; test = false)
     */
    public abstract void setFilteredData(ConsumerEventsData eventsData, long dateTrain, boolean train);
    
    /**
     * Concatenates events with events from altData. 
     * @param consumerEventsData
     */
    public abstract void concatenate(ConsumerEventsData altData);
    
    public abstract void removeDataTime(EventsDataDescriptor descriptor);
}
