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

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates types of events and provides their identifiers and ConsumerEventsData generators.
 * Types of events for AI are different from types of events or types of event data in the
 * database, don't confuse them.
 */
public enum EventType {
    RATING("ratings", new EventsDataGenerator() {
        @Override
        public ConsumerEventsData create(final DataStore dataStore, final String[] meta, final String formula) {
            return new ConsumerRatingsData(meta);
        }
    }),
    BUY("buys", new EventsDataGenerator() {
        @Override
        public ConsumerEventsData create(final DataStore dataStore, final String[] meta, final String formula) {
            return new ConsumerBuysData(meta);
        }
    }),
    COMPARE("compares", new EventsDataGenerator() {
        @Override
        public ConsumerEventsData create(final DataStore dataStore, final String[] meta, final String formula) {
            return new ConsumerComparesData(meta);
        }
    });

    private static final Logger logger = LogManager.getLogger(EventType.class);

    private static final Map<String, EventType> identifierMapping = new HashMap<String, EventType>();
    static
    {
        for (final EventType t : values())
        {
            identifierMapping.put(t.identifier, t);
        }
    }

    /**
     * The identifier of the event type, as is used in settings.
     */
    public final String identifier;
    /**
     * The generator of ConsumerEventsData instances for this event type.
     */
    public final EventsDataGenerator generator;

    EventType(final String identifier, final EventsDataGenerator generator) {
        this.identifier = identifier;
        this.generator = generator;
    }

    /**
     * Finds the EventType instance based on its identifier.
     *
     * @param identifier
     * @return the EventType instance with the specified identifier, or null if there is no such instance
     */
    public static EventType fromIdentifier(final String identifier) {
        final EventType t = identifierMapping.get(identifier);
        if (t == null)
            logger.error("There is no such EventType: " + identifier);
        return t;
    }
}
