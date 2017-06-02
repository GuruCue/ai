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
import java.util.HashMap;
import java.util.Map;

import de.congrace.exp4j.Calculable;
import de.congrace.exp4j.ExpressionBuilder;
import de.congrace.exp4j.UnknownFunctionException;
import de.congrace.exp4j.UnparsableExpressionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Contains properties of an event type per a DataStore instance. In theory
 * there can be many DataStore instances using the same event type, but each
 * having different properties for the same event type (e.g. the meta
 * variable).
 * This is probably not the final solution, because it contains a few
 * properties that are used by some ConsumerEventsData classes and not by
 * others.
 */
public class EventsDataDescriptor implements Serializable {
	private static final long serialVersionUID = 2342857215597098321L;
    private static final Logger logger = LogManager.getLogger(EventsDataDescriptor.class);
    public final int index; // index into the ConsumerEventsData array
    protected final EventsDataGenerator generator;
    public final DataStore dataStore;
    public final String[] meta;
    public final Map<String, Integer> metaIndices;
    public final String formula;
    private final Calculable doubleToByte; // a function that translates ratings from database to byte
    public long last_read_id; // last event that was processed by learner; used in incremental learning
    public long last_read_date; // last date of event that was processed by learner; used in incremental learning (in seconds from epoch)
    public int batch_size; // size of a batch in incremental learning
    public byte min, max; // min and max values of ratings, ratings should be positive
    public int keep_in_memory; // number of kept events in memory for each user
    public long keep_in_memory_time; // number of kept events in memory for each user constrained by time (remove events older that this)

    public EventsDataDescriptor(final int index, final EventsDataGenerator generator, final DataStore dataStore, final String[] meta, final String formula, int batch_size, final long last_read_id, final long last_read_date, final int keep_in_memory, final long keep_in_memory_time) {
        this.index = index;
        this.generator = generator;
        this.dataStore = dataStore;
        this.meta = meta;
        // make a map for meta index
        metaIndices = new HashMap<String, Integer> ();
        for (int i=0; i<meta.length; i++)
            metaIndices.put(meta[i], i);
        this.formula = formula;
        this.last_read_id = last_read_id;
        this.last_read_date = last_read_date;
        this.batch_size = batch_size;
        this.keep_in_memory = keep_in_memory;
        this.keep_in_memory_time = keep_in_memory_time;
        if ((null == formula) || (formula.length() == 0))
            this.doubleToByte = null;
        else
        {
            try {
                doubleToByte = new ExpressionBuilder(formula).withVariableNames("rating").build();
            } catch (UnknownFunctionException e) {
                final String reason = "Unknown function in exp4j expression: " + e.toString();
                logger.error(reason, e);
                throw new IllegalStateException(reason, e);
            } catch (UnparsableExpressionException e) {
                final String reason = "Unparsable exp4j expression: " + e.toString();
                logger.error(reason, e);
                throw new IllegalStateException(reason, e);
            } catch (RuntimeException e) {
                logger.error("Unexpected exp4j exception for formula \"" + formula + "\": " + e.toString(), e);
                throw e;
            }
        }
        this.min = -1;
        this.max = -1;
    }

    public ConsumerEventsData createEventsData() {
        return generator.create(dataStore, meta, formula);
    }

    public byte doubleToByte(final double drating) {
        doubleToByte.setVariable("rating", drating);
        return (byte) doubleToByte.calculate();
    }
    
    public int getMetaIndex(String m)
    {
        if (!metaIndices.containsKey(m))
            return -1;
        return metaIndices.get(m);
    }
}
