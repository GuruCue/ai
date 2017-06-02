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
package com.gurucue.recommendations.prediction.similarity;

import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.set.TIntSet;

/**
 * Jaccard similarity between two sets.
 * 
 */
public class JaccardCollaborativeSimilarity extends CollaborativeSimilarity {
    public final float BLEND;
    
    public JaccardCollaborativeSimilarity(String name, Settings settings)
    {
        BLEND = settings.getSettingAsFloat(name + "_BLEND");
    }
  
    /**
     * Computes the Jaccard distance in an optimized way: caches the array
     * representation of a set. One set is used as an array to iterate over
     * its values, the other set is used as is to check whether it contains
     * a given value. This way the size of an intersection is computed. If
     * neither of the arguments already contains a cached array, then the
     * first argument's array is created, cached, and used.
     * <p>
     * <strong>Important</strong>: because arrays are cached, the sets
     * must not change while computing distances between them. There is no
     * mechanism to detect changes to sets in order to recompute arrays.
     * <p>
     * The structure of arguments is simple because ordinarily the distance
     * computation is performed after all sets have been computed.
     * <p>
     * Instead of an array we could use an iterator() or forEach(), but both
     * have been proven far worse than the cached toArray() approach.
     * 
     * @param c1 first collection
     * @param c2 second collection
     * @return the Jaccard distance between the given collections
     */
    public float computeSim(CollaborativeSimilarity.Collection c1, CollaborativeSimilarity.Collection c2) {
        // take shorter one for iteration
        int[] iterand = (c1.iarray.length < c2.iarray.length) ? c1.iarray : c2.iarray;
        TIntSet verifier = (c1.iarray.length < c2.iarray.length) ? c2.iset : c1.iset;

        int intersectionSize = 0;
        for (int i = iterand.length - 1; i >= 0; i--) {
            if (verifier.contains(iterand[i])) intersectionSize++;
        }

        int unionSize = c1.iset.size() + c2.iset.size() - intersectionSize; // simple enough
        
        return ((float)intersectionSize)/(unionSize + BLEND);
    }
}
