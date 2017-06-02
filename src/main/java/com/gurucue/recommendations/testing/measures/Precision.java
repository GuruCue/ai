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

public class Precision implements Measure {
    int nconsumers;
    double precision;
    
    public Precision()
    {
        nconsumers = 0;
        precision = 0.0;
    }
    
    @Override
    public void addConsumerResults(int consumerIndex,
            TLongList relevantProducts, TByteList ratings, TDoubleList predictions,
            TLongList recommendedProducts,double recommendingTime) {
        nconsumers++;
        TLongHashSet relevant = new TLongHashSet (relevantProducts);
        TLongHashSet recommended = new TLongHashSet (recommendedProducts);
        relevant.retainAll(recommended);
        if (recommendedProducts.size() > 0)
            precision += ((float) relevant.size()) / recommendedProducts.size();
    }
    
    @Override
    public Measure clone() throws CloneNotSupportedException
    {
        return new Precision();
    }

    @Override
    public void finalizeComputations() {
        precision /= nconsumers;
    }
    
    @Override
    public String toString() {
        return "precision: " + precision + "\n";
    }
}