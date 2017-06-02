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

import gnu.trove.map.hash.TLongIntHashMap;


/**
 * Coldstart data, consumer selects which of the products he prefers
 * 
 * 
 */
@Deprecated
public class CSPairData {
	public int product1Index; // first selected product
	public int product2Index; // second product
    //selected=0: product1 was selected
    //selected=1: product2 was selected
    //selected=-1: no product was selected
	public byte selected;
	
	public CSPairData(int product1Index, int product2Index, byte selected){
		this.product1Index=product1Index;
		this.product2Index=product2Index;
		this.selected=selected;
	}
	
	/**
	 * Raw constructor. Data provided are the value from the DB.
	 * @param product1Index
	 * @param product2Index
	 * @param selected
	 */
	public CSPairData(Long product1Index, Long product2Index, Long selected, TLongIntHashMap prodMap)
	{
	    this.product1Index = prodMap.get((int) product1Index.longValue());
        this.product2Index = prodMap.get((int) product2Index.longValue());
        if (prodMap == null || selected == null)
            this.selected = -1;
        else
        {
            int sel_index = prodMap.get((int) selected.longValue());
            if (this.product1Index == sel_index)
                this.selected = 0;
            else if (this.product2Index == sel_index)
                this.selected = 1;
            else
                this.selected = -1;
        }
	}
	

	public boolean containsProduct(int productIndex)
	{
	    return (productIndex == this.product1Index || productIndex == this.product2Index);
	}
}
