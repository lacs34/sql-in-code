package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.SourceReference
import com.losttemple.sql.language.types.*

class LimitSet<T>(private val sourceSet: DbSet<T>, private val count: Int): DbSet<T>, SqlSet {
    override val description: T = sourceSet.wrapDescription(DummyPrefix(this))

    override fun wrapDescription(prefix: SetPrefix): T {
        return sourceSet.wrapDescription(prefix)
    }

    override val set: SqlSet
        get() = this

    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        constructor.limit = count
    }

    fun offset(offset: Int): DbSet<T> {
        return LimitWithOffsetSet(sourceSet, count, offset)
    }
}

fun <T> DbSet<T>.limit(count: Int): LimitSet<T> {
    return LimitSet<T>(this, count)
}

class LimitWithOffsetSet<T>(private val sourceSet: DbSet<T>,
                            private val count: Int,
                            private val offset: Int): DbSet<T>, SqlSet {
    override val description: T = sourceSet.wrapDescription(DummyPrefix(this))

    override fun wrapDescription(prefix: SetPrefix): T {
        return sourceSet.wrapDescription(prefix)
    }

    override val set: SqlSet
        get() = this

    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        constructor.limitWithOffset(count, offset)
    }
}

fun <T> DbSet<T>.limitWithOffset(count: Int, offset: Int): DbSet<T> {
    return LimitWithOffsetSet<T>(this, count, offset)
}