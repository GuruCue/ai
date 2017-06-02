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
 * Cosine similarity between two sets (usually two items with indices who bought these items).
 */
public class CosineCollaborativeSimilarity extends CollaborativeSimilarity {
    public final float BLEND;
    
    public CosineCollaborativeSimilarity(String name, Settings settings)
    {
        BLEND = settings.getSettingAsFloat(name + "_BLEND");
    }
    
    @Override
    public float computeSim(Collection c1, Collection c2) {
        // take shorter one for iteration
        int[] iterand = (c1.iarray.length < c2.iarray.length) ? c1.iarray : c2.iarray;
        TIntSet verifier = (c1.iarray.length < c2.iarray.length) ? c2.iset : c1.iset;
        
        int intersectionSize = 0;
        for (int i = iterand.length - 1; i >= 0; i--) {
            if (verifier.contains(iterand[i])) intersectionSize++;
        }

        return (float) (((float)intersectionSize)/(Math.sqrt(c1.iarray.length * c2.iarray.length) + BLEND));
    }
}
