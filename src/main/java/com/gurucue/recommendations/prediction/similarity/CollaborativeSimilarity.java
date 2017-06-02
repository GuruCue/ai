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
import com.gurucue.recommendations.misc.Misc;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Similarity used in item or user-based collaborative kNNs.
 *
 */
public abstract class CollaborativeSimilarity {
    /**
     * Computes similarity between collections v1 and collections v2.
     * @param c1 
     * @param c2
     * @return similarity between c1 and c2.
     */
    public abstract float computeSim(Collection c1, Collection c2);
    
    
    /**
     * The method creates an arbitrary object of similarity type.
     * 
     * @param similarityName
     * @return
     */
    public static CollaborativeSimilarity getSimilarity(String name, Settings settings)
    {
        String className = settings.getSetting(name + "_CLASS");
        return (CollaborativeSimilarity) Misc.createClassObject(className, name, settings);
    }
    
    /**
     * The class that is used for arguments to optimized distance
     * computation method that caches the array representation
     * of the set.
     * <p>
     * The usage of instances of this class should be two-step:
     * first the set should be computed, and after that is should
     * not change any more, and the array representation is made
     * from the set. Distances should be calculated in the second
     * step.
     */
    public static class Collection {
        public TIntSet iset;
        public int [] iarray; 
        
        public Collection() {
            iset = new TIntHashSet();
        }
        
        @Override
        public Object clone ()  throws CloneNotSupportedException {
            Collection o = (Collection) super.clone();
            o.iset = new TIntHashSet(iset);
            o.iarray = iarray.clone();
            return o;
        }
    }
}
