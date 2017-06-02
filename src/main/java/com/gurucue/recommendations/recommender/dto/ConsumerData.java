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
import java.util.Map;

/**
 * Class contains data about a user (consumer). 
 * 
 */
public class ConsumerData implements Serializable {
    private static final long serialVersionUID = 7785855401564092089L;
    
    public long consumerId;
    public ConsumerEventsData[] events; // indexed by eventTypeIndex
	
	public ConsumerData() 
	{}
    
    public ConsumerData(final long consumerId, final int eventTypeCount) {
		this.consumerId = consumerId;
        this.events = new ConsumerEventsData[eventTypeCount];
	}

    /**
     * Copy constructor.
     *
     * @param original
     * @param emptyEvents whether to create an empty copy, not copying any events
     */
    public ConsumerData(final ConsumerData original, final boolean emptyEvents) {
        consumerId = original.consumerId;
        events = new ConsumerEventsData[original.events.length];
        for (int i = events.length - 1; i >= 0; i--)
        {
            if (null != original.events[i])
                events[i] = original.events[i].createCopy(emptyEvents);
        }
    }
    
    public void concatenate(ConsumerData alt)
    {
        for (int i = events.length - 1; i >= 0; i--)
        {
            if (null == alt.events[i])
                continue;
            if (null == events[i])
                events[i] = alt.events[i].createCopy(false);
            else
                events[i].concatenate(alt.events[i]);
        }
    }
    
    public void removeData(final Map<String, EventsDataDescriptor> eventsDescriptors)
    {
        for (Map.Entry<String, EventsDataDescriptor> entry : eventsDescriptors.entrySet())
        {
            final ConsumerEventsData event = events[entry.getValue().index];
            if (event == null)
                continue;
            final int keep_in_memory = entry.getValue().keep_in_memory;
            event.removeData(keep_in_memory);
        }
    }

    public void removeDataTime(
            Map<String, EventsDataDescriptor> eventsDescriptors) {
        for (Map.Entry<String, EventsDataDescriptor> entry : eventsDescriptors.entrySet())
        {
            final ConsumerEventsData event = events[entry.getValue().index];
            if (event == null || event.n == 0)
                continue;
            final EventsDataDescriptor descriptor = entry.getValue();
            final long keep_in_memory_time = descriptor.keep_in_memory_time;
            if (keep_in_memory_time > -1)
                event.removeDataTime(descriptor);
        }
    }
}
