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
}

fun <T> DbSet<T>.limit(count: Int): DbSet<T> {
    return LimitSet<T>(this, count)
}