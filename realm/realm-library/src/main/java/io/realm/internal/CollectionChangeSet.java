/*
 * Copyright 2017 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.realm.internal;

import io.realm.OrderedCollectionChange;

/**
 * Implementation of {@link OrderedCollectionChange}. This class holds a pointer to the Object Store's
 * CollectionChangeSet and read from it only when needed. Creating an Java object from JNI when the collection
 * notification arrives is avoided since we also support the collection listeners without a change set parameter,
 * parsing the change set may not be necessary all the time.
 */
public class CollectionChangeSet implements OrderedCollectionChange, NativeObject {

    private static long finalizerPtr = nativeGetFinalizerPtr();
    private final long nativePtr;

    private long[] deletionIndices;
    private long[] insertionIndices;
    private long[] changeIndices;
    private Range[] deletionRanges;
    private Range[] insertionRanges;
    private Range[] changeRanges;

    public CollectionChangeSet(long nativePtr) {
        this.nativePtr = nativePtr;
        Context.dummyContext.addReference(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] getDeletions() {
        if (deletionIndices == null) {
           deletionIndices = rangesToIndexArray(getDeletionRanges());
        }
        return deletionIndices.length == 0 ? null : deletionIndices;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] getInsertions() {
        if (insertionIndices == null) {
            insertionIndices = rangesToIndexArray(getInsertionRanges());
        }
        return insertionIndices.length == 0 ? null : insertionIndices;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] getChanges()  {
        if (changeIndices == null) {
            changeIndices = rangesToIndexArray(getChangeRanges());
        }
        return changeIndices.length == 0 ? null : changeIndices;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Range[] getDeletionRanges() {
        if (deletionRanges == null) {
            deletionRanges = longArrayToRangeArray(nativeGetDeletionRanges(nativePtr));
        }

        return deletionRanges.length == 0 ? null : deletionRanges;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Range[] getInsertionRanges() {
        if (insertionRanges == null) {
            insertionRanges = longArrayToRangeArray(nativeGetInsertionRanges(nativePtr));
        }

        return insertionRanges.length == 0 ? null : insertionRanges;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Range[] getChangeRanges() {
        if (changeRanges == null) {
            changeRanges = longArrayToRangeArray(nativeGetChangeRanges(nativePtr));
        }

        return changeRanges.length == 0 ? null : changeRanges;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNativePtr() {
        return nativePtr;
    }

    @Override
    public long getNativeFinalizerPtr() {
        return finalizerPtr;
    }

    // Convert long array returned by the nativeGetXxxRanges() to Range array.
    private Range[] longArrayToRangeArray(long[] longArray) {
        if (longArray == null) {
            // Returns a size 0 array so we know the JNI gets called.
            return new Range[0];
        }

        Range[] ranges = new Range[longArray.length / 2];
        for (int i = 0; i < ranges.length; i++) {
            ranges[i] = new Range(longArray[i * 2], longArray[i * 2 + 1]);
        }
        return ranges;
    }

    // Convert Range array to array of indices.
    private long[] rangesToIndexArray(Range[] ranges) {
        if (ranges == null || ranges.length == 0) {
            return new long[0];
        }

        long count = 0;
        for (Range range : ranges) {
            count += range.length;
        }
        if (count > Integer.MAX_VALUE) {
            throw new IllegalStateException("There are too many indices in this change set. " +
                    "They cannot fit into an array.");
        }

        long[] indexArray = new long[(int) count];
        int i = 0;
        for (Range range : ranges) {
            for (int j = 0; j < range.length; j++) {
                indexArray[i] = range.startIndex + j;
                i++;
            }
        }
        return indexArray;
    }

    private native static long nativeGetFinalizerPtr();
    // Returns the ranges as an long array. eg.: [startIndex1, length1, startIndex2, length2, ...]
    private native static long[] nativeGetDeletionRanges(long nativePtr);
    private native static long[] nativeGetInsertionRanges(long nativePtr);
    private native static long[] nativeGetChangeRanges(long nativePtr);
}
