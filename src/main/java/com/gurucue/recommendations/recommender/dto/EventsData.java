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
import java.util.Iterator;

/**
 * Abstract class for all type of events. 
 */
@Deprecated
public abstract class EventsData {
    public static final String RATINGS = "ratings";
    public static final String BUYS = "buys";
    public static final String COMPARES = "compares";
    
    protected final DataStore data;
    protected final String [] metaDesc;
    protected final String formula;

    /**
     * @param data 
     * @param meta Which meta attributes of an event should also be internally stored?
     * @param formula A string representation of formula that maps from database value of event to internal value (usually byte). 
     * For BuysData and ComparesData, formula is not needed and not used.
     */
    public EventsData(final DataStore data, final String [] meta, final String formula)
    {
        this.data = data;
        this.metaDesc = meta;
        this.formula = formula;
    }
    
    /**
     * Adds an event to consumer.
     * @param consumerIndex Index of consumer in the array.
     * @param result Result of one event (an array of Objects)
     */
    public abstract void addEvent(int consumerIndex, Object [] result);
    
    /**
     * No more data will be added to consumers; needed to trim the arrays.
     */
    public abstract void finalizeReadingEvents();
    
    /**
     * Returns the events of the i-th user.
     * @param i
     * @return
     */
    public abstract ConsumerEventsData getEvent(int i);

    public abstract Iterator<? extends ConsumerEventsData> eventIterator();
    
    /**
     * Return the number of all events of all users.
     * Compute on-demand.
     * @return
     */
    public long getN()
    {
        long nEvents = 0;
        final Iterator<? extends ConsumerEventsData> events = eventIterator();
        while (events.hasNext())
            nEvents += events.next().n;
        return nEvents;
    }
    
    /**
     * Return number of consumers.
     * @return
     */
    public abstract int getNConsumers();

    /**
     * Creates a subset of events from events
     * @param eventsData Dull data on events.
     * @param dateTrain events before and on that moment are train data, others test data
     * @param train (train = true; test = false)
     */
    public abstract void setFilteredData(EventsData eventsData, long dateTrain, boolean train);
    
    /**
     * Adds a new consumer to the data. Required for updateConsumer functionality. 
     * A new consumer is added at the end of the ConsumerData list. 
     */
    public abstract void addConsumer();
    /**
     * Resets and deletes all data about a consumer. It basically means that all events are deleted 
     * and statistics reseted. Used in consumer update.
     */
    public abstract void resetConsumer(int consumerIndex);
    
    /**
     * Creates a learn data set for cross-validation. It contains all data from indices, except those specified by fold.
     * @param indices
     * @param fold
     * @param eventsData
     * @return
     */
    public abstract void setFilteredData(byte[] indices, byte fold, EventsData eventsData);
}
