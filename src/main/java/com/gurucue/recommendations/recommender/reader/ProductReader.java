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

import com.gurucue.recommendations.ProcessingException;
import com.gurucue.recommendations.ResponseException;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.value.AttributeValues;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ProductData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser and setter of Product attributes for {@link ProductData} instances.
 * It is set up in the constructor, and exposes the {@link #readFields(ProductData, ProductData, String, String, String)}
 * method to set values to ProductData from product's JSON fields <code>attributes</code>
 * and <code>related</code>, and from the field <code>partner_product_code</code>.
 */
public final class ProductReader {
    private static final Logger log = LogManager.getLogger(ProductReader.class);

    private final String[] attributeNames;
    private final String fieldDefinitions;
    private final DataProvider provider;
    private final List<FieldReader> attributeReaders;
    private final List<FieldReader> relatedReaders;
    private final StringBuilder processingLog = new StringBuilder(512);
    private final int partnerProductCodeIndex;

    public ProductReader(
            final String[] attributeNames, // from ATTRIBUTE_NAMES
            final String fieldDefinitions, // from READ_ATTRIBUTES_FIELDS
            final DataProvider provider
    ) {
        this.attributeNames = attributeNames;
        this.fieldDefinitions = fieldDefinitions;
        this.provider = provider;

        final String[] defs = fieldDefinitions.split(",");
        final AttributeCodes attributes = provider.getAttributeCodes();
        final int n = defs.length;
        attributeReaders = new ArrayList<>(n);
        relatedReaders = new ArrayList<>(n);
        int partnerProductCodeIndex = -1;

        for (int i = 0; i < n; i++) {
            final String[] def = defs[i].split(":"); // [attribute-name, index-name]
            final String attributeIdentifier = def[0];
            final int index = attributeIndex(def[1]);

            if (attributeIdentifier.startsWith("$")) { // meta-fields start with $
                // product meta-data, i.e. table fields
                if ("$partner_product_code".equals(attributeIdentifier)) {
                    partnerProductCodeIndex = index;
                }
                else {
                    log.error("There is no product field " + attributeIdentifier);
                }
            }
            else {
                // product attributes
                final Attribute a = attributes.byIdentifier(attributeIdentifier);

                if (a.getIsMultivalue()) {
                    attributeReaders.add(new MultiValueFieldReader(a, index));
                } else {
                    switch (attributeIdentifier) {
                        case "video-id":
                        case "series-id":
                            relatedReaders.add(new SingleValueFieldReader(a, index));
                            break;
                        case "begin-time":
                        case "end-time":
                            attributeReaders.add(new TimestampFieldReader(a, index));
                            break;
                        default:
                            attributeReaders.add(new SingleValueFieldReader(a, index));
                            break;
                    }
                }
            }
        }

        this.partnerProductCodeIndex = partnerProductCodeIndex;
    }

    private int attributeIndex(final String indexName) {
        for (int i = attributeNames.length - 1; i >= 0; i--) {
            if (attributeNames[i].equalsIgnoreCase(indexName)) return i;
        }
        throw new ProcessingException("There is no attribute index named \"" + indexName + "\"");
    }

    public void readFields(final ProductData newData, final ProductData oldData, final String attributesJson, final String relatedJson, final String partnerProductCode) {
        // process JSON attributes
        readJsonFields(newData, oldData, attributesJson, attributeReaders);
        readJsonFields(newData, oldData, relatedJson, relatedReaders);
        // process any meta-data fields
        if (partnerProductCodeIndex >= 0) readMetaField(newData, oldData, partnerProductCodeIndex, partnerProductCode);
    }

    private void readJsonFields(
            final ProductData newData,
            final ProductData oldData,
            final String json,
            final List<FieldReader> readers
    ) {
        if (readers.size() > 0) {
            final AttributeValues attributes;
            try {
                attributes = new AttributeValues(json, provider, processingLog);
            } catch (ResponseException e) {
                throw new ProcessingException("Failed to parse JSON data: " + e.toString(), e);
            }

            if (processingLog.length() > 0) {
                log.warn(processingLog.toString());
                processingLog.delete(0, processingLog.length());
            }

            for (final FieldReader reader : readers) {
                final int index = reader.getAttributeIndex();
                if ((oldData != null) && (newData.getAttribute(index) == null)) {
                    // recycle the old structure
                    final Attr attr = oldData.getAttribute(index);
                    if (attr != null) newData.setAttribute(index, attr);
                }
                reader.process(newData, attributes);
            }
        }
    }

    private void readMetaField(
            final ProductData newData,
            final ProductData oldData,
            final int index,
            final String value
    ) {
        if ((oldData != null) && (newData.getAttribute(index) == null)) {
            // recycle the old structure
            final Attr attr = oldData.getAttribute(index);
            if (attr != null) newData.setAttribute(index, attr);
        }
        if (value == null) return;
        newData.setAttrValue(index, value);
    }
}
