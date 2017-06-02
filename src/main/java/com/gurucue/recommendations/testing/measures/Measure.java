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
package com.gurucue.recommendations.testing.measures;

import gnu.trove.list.TByteList;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TLongList;

/**
 * Interface Measure. Every Measure class used in EvaluationResult should implement these methods.
 */
public interface Measure
{
    /**
     * Adds recommended data for a consumer.
     * 
     * @param consumerIndex Index of the consumer in the DataStore lists.
     * @param relevantProducts Products relevant for this consumer (usually consumed products).
     * @param recommendedProducts Products recommended to consumer.
     * @param predictions Prediction values computed by the recommender (order should be the same as in recommendedProducts).
     * @param recommendingTime Time used by the recommender to produce this recommendation.
     */
    public void addConsumerResults(int consumerIndex, TLongList relevantProducts, TByteList ratings, TDoubleList predictions, TLongList recommendedProducts, double recommendingTime);
    
    /**
     * When all evaluation data (calls to addConsumerResults) are added,
     * call this to finalize results: do the final computations. 
     */
    public void finalizeComputations();
    public Measure clone() throws CloneNotSupportedException;
    public String toString();
}