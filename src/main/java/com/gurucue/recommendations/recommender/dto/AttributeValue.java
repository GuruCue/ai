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

import com.gurucue.recommendations.entity.Attribute;

/**
 * This class is used in MasterRecommender to specify, which products are relevant for classification. 
 * Currently we can only constrain by attribute-value pair, but in future it might be useful to have 
 * range (for continuous) or sets of values (for multivalue attributes).
 */
public class AttributeValue {
    Attribute attribute;
    String value; 
    
    public AttributeValue(Attribute attribute, String value)
    {
        this.attribute = attribute;
        this.value = value;
    }
    
    Attribute getAttribute()
    {
        return attribute;
    }
    
    String getValue()
    {
        return value;
    }
}
