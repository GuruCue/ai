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

public class Pair implements Comparable<Pair> {

	  public Double left;
	  public Integer right;

	  public Pair(Double left, Integer right) {
	    this.left = left;
	    this.right = right;
	  }

	  public Double getLeft() { return left; }
	  public Integer getRight() { return right; }
	  
	  public void setPair(Double left, Integer right) {
		  this.left = left;
		  this.right = right;
	  }

	  public int hashCode() { return left.hashCode() ^ right.hashCode(); }

	  public boolean equals(Object o) {
	    if (o == null) return false;
	    if (!(o instanceof Pair)) return false;
	    Pair pairo = (Pair) o;
	    return this.left.equals(pairo.getLeft()) &&
	           this.right.equals(pairo.getRight());
	  }
	  
	 public int compareTo(Pair o) {
		 if (left < o.left) return -1;
		 if (left > o.left) return 1;
		 return 0;
	 }

}
