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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import gnu.trove.set.TIntSet;

/**
 * Class contains data about a product and some basic statistics related to the
 * product.
 *
 */
public class ProductData{
    private static org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ProductData.class);
	
    public final long productId; // product id in database
    public final long publicProductId; // product id in database
    private Attr[] attr; // attributes of the product
    public int productType; // product types, e.g. movie, tv-series, etc.
    private Domain domain;

    public int freq; // number of occurrences of this product in events

	public ProductData(final Long productId, final Long publicProductId, int productType, final Domain domain) {
	    if (publicProductId == null)
	        this.publicProductId = -1;
	    else
            this.publicProductId = publicProductId;
        this.productId = productId;
		this.productType = productType;
		this.domain = domain;
		attr = null;
		freq = 0;
	}

    /**
     * Copy constructor.
     *
     * @param original
     */
    public ProductData(final ProductData original) {
        productId = original.productId;
        publicProductId = original.publicProductId;
        attr = original.attr;
        productType = original.productType;
        freq = original.freq;
        domain = original.domain;
    }
    
    public TIntSet getValues(int attrIndex)
    {
        if (attr == null || attr[attrIndex] == null || !attr[attrIndex].hasValue())
            return null;
        return attr[attrIndex].getValues(); 
    	
    }
    
    public Long getLongAttrValue(int attrIndex)
    {
        if (attr == null || attr[attrIndex] == null || !attr[attrIndex].hasValue())
            return null;
        return ((LongAttr) attr[attrIndex]).value; 
    }

    public Float getFloatAttrValue(int attrIndex)
    {
        if (attr == null || attr[attrIndex] == null || !attr[attrIndex].hasValue())
            return null;
        return ((FloatAttr) attr[attrIndex]).value; 
    }

    public String getStrAttrValue(int attrIndex, int attrValue)
    {
        if (attr == null || attr[attrIndex] == null)
            return domain.attrs[attrIndex].getStrValue(attrValue);
        return attr[attrIndex].getStrValue(attrValue); 
    }
    
    public Attr [] getAttributes()
    {
    	return attr;
    }

    public Attr getAttribute(int ati)
    {
    	if (attr == null)
    		return null;
    	return attr[ati];
    }
    
    public void setAttributes(Attr [] attr)
    {
    	this.attr = attr;
    }
    
    public void setAttribute(int ati, Attr at)
    {
    	if (attr == null)
    		domain.createAttributeProduct(this, ati);
    	attr[ati] = at;
    }
    
    public void setAttrValue(int ati, String value)
    {
    	if (attr == null || attr[ati] == null)
    		domain.createAttributeProduct(this, ati);
    	attr[ati].setValue(value);
    }
    
    public String getAttributeValues()
    {
    	if (attr == null)
    		return "";
    	StringBuilder sb = new StringBuilder();
    	for (Attr a : attr)
    	{
    		if (a == null)
    			continue;
    		sb.append(a.name);
    		sb.append("=");
    		sb.append(a.toString());
    		sb.append(",");
    	}
    	
    	return sb.toString();
    }
    
    public static ProductData deserialize(ObjectInputStream in, Map<String, Object> globals, Domain domain) throws IOException, ClassNotFoundException
    {
    	// read basic values
    	final Long tprodID = (Long) in.readObject();
    	if (tprodID == null) // product data was null
    		return null;
    	final Long tfinProdID = in.readLong();
    	final int tprodType = in.readInt();
    	ProductData newData = new ProductData(tprodID, tfinProdID, tprodType, domain);
    	
    	Object hasAttr = in.readObject();
    	if (hasAttr == null)
    		newData.setAttributes(null);
    	else {
    		for (int ati = 0; ati < domain.attrs.length; ati++) 
    		{
    	    	hasAttr = in.readObject();
    	    	if (hasAttr != null)
    	    	{
    	    		domain.createAttributeProduct(newData, ati);
    	    		newData.getAttribute(ati).loadFromFile(in, globals);
    	    	}
    		}
    	}
    	return newData;
    }
    
    public void serialize(ObjectOutputStream out) throws IOException
    {
    	out.writeObject(Long.valueOf(productId)); // must write as an object!!! for null comparison if product data was null
    	out.writeLong(publicProductId);
    	out.writeInt(productType);
    	if (attr == null)
    		out.writeObject(null);
    	else {
    		out.writeObject("not null");
	    	for (Attr a : attr)
	    	{
	    		if (a == null)
	    			out.writeObject(null);
	    		else {
	    			out.writeObject("not null");
	    			a.saveToFile(out);
	    		}
	    	}
    	}
    }

}
