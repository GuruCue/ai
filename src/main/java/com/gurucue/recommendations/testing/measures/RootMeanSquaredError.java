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

import gnu.trove.list.TDoubleList;
import gnu.trove.list.TByteList;
import gnu.trove.list.TLongList;

public class RootMeanSquaredError implements Measure {
    int nitems;
    double mse;
    
    public RootMeanSquaredError()
    {
        nitems = 0;
        mse = 0.0;
    }
    
    @Override
    public void addConsumerResults(int consumerIndex,
            TLongList relevantProducts, TByteList ratings, TDoubleList predictions,
            TLongList recommendedProducts,double recommendingTime) {
        if (relevantProducts.size() == 0)
            return;
        nitems += relevantProducts.size();
        final int relSize = relevantProducts.size();
        for (int i=0; i<relSize; i++)
            mse += Math.pow(ratings.get(i)-predictions.get(i), 2);
    }
    
    @Override
    public Measure clone() throws CloneNotSupportedException
    {
        return new RootMeanSquaredError();
    }

    @Override
    public void finalizeComputations() {
        mse /= nitems;
        mse = Math.sqrt(mse);
    }
    
    @Override
    public String toString() {
        return "root mean squarred error: " + mse + "\n";
    }
}
