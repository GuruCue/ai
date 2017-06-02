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
package com.gurucue.recommendations.recommender;

/**
 * A do-nothing commit. For cases where nothing needs to be updated.
 */
public class EmptyCommit implements Commitable {
    public static final EmptyCommit INSTANCE = new EmptyCommit();

    /**
     * Don't allow instantiation: this is a singleton class.
     * There is no reason for existence of multiple instances: they would
     * all be exactly equal in function.
     */
    private EmptyCommit () {

    }

    @Override
    public void commit() {

    }
}
