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
import gnu.trove.set.hash.TLongHashSet;

public class MeanAveragePrecision implements Measure {
    int nconsumers;
    double map;
    
    public MeanAveragePrecision()
    {
        nconsumers = 0;
        map = 0.0;
    }
    
    @Override
    public void addConsumerResults(int consumerIndex,
            TLongList relevantProducts, TByteList ratings, TDoubleList predictions,
            TLongList recommendedProducts,double recommendingTime) {
        if (relevantProducts.size() == 0 || recommendedProducts.size() == 0)
            return;
        nconsumers++;
        TLongHashSet intersection = new TLongHashSet ();
        TLongHashSet relevant = new TLongHashSet (relevantProducts);
        double ap = 0.0;
        for (int k=0; k<recommendedProducts.size(); k++)
        {
            if (relevant.contains(recommendedProducts.get(k)))
            {
                intersection.add(recommendedProducts.get(k));
                ap += ((float) intersection.size()) / (k+1);
            }
        }
        ap /= recommendedProducts.size();
        map += ap;
    }
    
    @Override
    public Measure clone() throws CloneNotSupportedException
    {
        return new MeanAveragePrecision();
    }

    @Override
    public void finalizeComputations() {
        map /= nconsumers;
    }
    
    @Override
    public String toString() {
        return "mean average precision: " + map + "\n";
    }
}