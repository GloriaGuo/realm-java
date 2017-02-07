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

#include "io_realm_internal_CollectionChangeSet.h"

#include <collection_notifications.hpp>

#include "util.hpp"

using namespace realm;

static void finalize_changeset(jlong ptr);
static jlongArray index_set_to_jlong_array(JNIEnv* env, const IndexSet& index_set);

static void finalize_changeset(jlong ptr)
{
    TR_ENTER_PTR(ptr);
    delete reinterpret_cast<CollectionChangeSet*>(ptr);
}

static jlongArray index_set_to_jlong_array(JNIEnv* env, const IndexSet& index_set)
{
    if (index_set.empty()) {
        return NULL;
    }

    std::vector<jlong> ranges_vector;
    for (auto& changes : index_set) {
        ranges_vector.push_back(changes.first);
        ranges_vector.push_back(changes.second - changes.first);
    }

    jlongArray jlong_array = env->NewLongArray(static_cast<jsize>(ranges_vector.size()));
    env->SetLongArrayRegion(jlong_array, 0, ranges_vector.size(), ranges_vector.data());
    return jlong_array;
}

JNIEXPORT jlong JNICALL
Java_io_realm_internal_CollectionChangeSet_nativeGetFinalizerPtr(JNIEnv*, jclass)
{
    TR_ENTER()
    return reinterpret_cast<jlong>(&finalize_changeset);
}

JNIEXPORT jlongArray JNICALL
Java_io_realm_internal_CollectionChangeSet_nativeGetDeletionRanges(JNIEnv* env, jclass, jlong native_ptr)
{
    TR_ENTER_PTR(native_ptr)
    // no throws
    auto& change_set = *reinterpret_cast<CollectionChangeSet*>(native_ptr);
    return index_set_to_jlong_array(env, change_set.deletions);
}

JNIEXPORT jlongArray JNICALL
Java_io_realm_internal_CollectionChangeSet_nativeGetInsertionRanges(JNIEnv* env, jclass, jlong native_ptr)
{
    TR_ENTER_PTR(native_ptr)
    // no throws
    auto& change_set = *reinterpret_cast<CollectionChangeSet*>(native_ptr);
    return index_set_to_jlong_array(env, change_set.insertions);
}

JNIEXPORT jlongArray JNICALL
Java_io_realm_internal_CollectionChangeSet_nativeGetChangeRanges(JNIEnv* env, jclass, jlong native_ptr)
{
    TR_ENTER_PTR(native_ptr)
    // no throws
    auto& change_set = *reinterpret_cast<CollectionChangeSet*>(native_ptr);
    return index_set_to_jlong_array(env, change_set.modifications_new);
}

