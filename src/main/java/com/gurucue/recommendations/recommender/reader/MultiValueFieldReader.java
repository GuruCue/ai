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
package com.gurucue.recommendations.recommender.reader;

import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.value.AttributeValues;
import com.gurucue.recommendations.recommender.dto.ProductData;

/**
 * Sets value(s) to a {@link ProductData} attribute; handles only attributes that
 * can contain multiple values.
 * @see ProductReader
 */
public class MultiValueFieldReader implements FieldReader {
    private final Attribute attribute;
    private final int attributeIndex;

    public MultiValueFieldReader(final Attribute attribute, final int attributeIndex) {
        this.attribute = attribute;
        this.attributeIndex = attributeIndex;
    }

    @Override
    public int getAttributeIndex() {
        return attributeIndex;
    }

    @Override
    public void process(final ProductData productData, final AttributeValues attributes) {
        final String[] vals = attributes.getAsStrings(attribute);
        if ((vals == null) || (vals.length == 0)) return;
        for (int i = vals.length - 1; i >= 0; i--) {
            productData.setAttrValue(attributeIndex, vals[i]);
        }
    }
}
