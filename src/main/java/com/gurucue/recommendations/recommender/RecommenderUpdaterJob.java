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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Holds parameters of a model computation job used in
 * <code>RecommenderUpdater</code>.
 */
public abstract class RecommenderUpdaterJob {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition finishedSignal = lock.newCondition();
    private boolean notFinished = true;

    abstract void work() throws InterruptedException;

    /**
     * Signal any threads waiting for the job completion that the job is
     * completed, and remember the fact for any future threads wanting
     * to wait for job completion.
     */
    void signalFinished() {
        lock.lock();
        try {
            notFinished = false;
            finishedSignal.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Mark the job not finished. Useful if the job gets reused.
     */
    void markUnfinished() {
        lock.lock();
        try {
            notFinished = true;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Waits for the job to be completed, if it's not completed yet.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting for the job completion
     */
    public void waitUntilFinished() throws InterruptedException {
        lock.lock();
        try {
            while (notFinished) {
                finishedSignal.await();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns the finished state of the job.
     * @return whether the job has completed
     */
    public boolean isFinished() {
        lock.lock();
        try {
            return !notFinished;
        }
        finally {
            lock.unlock();
        }
    }
}
