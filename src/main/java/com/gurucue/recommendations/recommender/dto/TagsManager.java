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

import java.util.Map;

public class TagsManager {
    public final static String MAX_RECOMMEND_TAG = "N_RECOMMEND";
    public final static String SECONDARY_TAG = "SECONDARY_PRODUCT_TAGS";
    public final static String PRIMARY_TAG = "PRODUCT_TAGS";
    private final static String CURRENT_TIME = "recommend_time";
    

    public static long getCurrentTimeSeconds(final Map<String, String> tags)
    {
        // get current time
        long currentTime;
        if (tags.containsKey(CURRENT_TIME))
            currentTime = Long.valueOf(tags.get(CURRENT_TIME));
        else
            currentTime = System.currentTimeMillis() / 1000;  
        return currentTime;
    }
}
