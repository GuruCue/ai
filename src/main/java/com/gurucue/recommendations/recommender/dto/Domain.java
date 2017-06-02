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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.recommender.reader.Reader;

/**
 * This class stores information about attributes of products that are used in generating recommendations. 
 * It also governs methods for general manipulations of attributes. 
 *
 */
public class Domain {
	final Attr [] attrs;
	
	public Domain(final Attr [] attrs)
	{
		this.attrs = attrs;
	}
	
	public void createAttributeProduct(ProductData p, int ati)
	{
		if (p.getAttributes() == null)
			p.setAttributes(new Attr[attrs.length]);
		p.setAttribute(ati, attrs[ati].clone());
	}
	
	public void serializeDomainAndProducts(List<ProductData> productData, ObjectOutputStream out) throws IOException
	{
		Map<String, Object> globals = new HashMap<String, Object> ();
   		for (Attr a : attrs)
   			a.storeGlobals(globals);
   		out.writeObject(globals);
   		
    	final int productsSize = productData.size();
    	out.writeInt(productsSize);
    	// write products
    	for (ProductData p : productData)
    	{
    		if (p == null)
    			out.writeObject(null);
    		else
    			p.serialize(out);
    	}   		
	}
	
	@SuppressWarnings("unchecked")
	public static Domain deserializeDomainAndProducts(List<ProductData> productData, Reader reader, ObjectInputStream in) throws ClassNotFoundException, IOException {
	    // read globals
	    Map<String, Object> globals = (Map<String, Object>) in.readObject();
	    
   		Domain domain = reader.createDomain();	
   		for (Attr a : domain.attrs)
   			a.loadGlobals(globals);
   		
	    // read products array
	    final int productsSize = in.readInt();
	    for (int i = 0; i < productsSize; i++)
	    {
	    	productData.add(ProductData.deserialize(in, globals, domain));
	    }

		return domain;
	}
	
	public Attr [] getAttrs()
	{
		return attrs;
	}
}
