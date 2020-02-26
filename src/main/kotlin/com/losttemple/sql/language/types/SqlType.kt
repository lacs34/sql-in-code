package com.losttemple.sql.language.types

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.generate.ReferenceConstructor
import com.losttemple.sql.language.generate.SourceReference
import com.losttemple.sql.language.operator.SqlDialect
import java.sql.ResultSet

enum class GenerateTarget {
    Join,
    Where,
    From
}

data class SourcePath(val path: String) {
    infix operator fun plus(another: String): SourcePath {
        return SourcePath(this.path + sep + another)
    }

    infix operator fun plus(another: SourcePath): SourcePath {
        return this + sep + another.path
    }

    fun pre(another: String): SourcePath {
        return SourcePath(another + sep + path)
    }

    fun startsWith(prefix: SourcePath): Boolean {
        return path.startsWith(prefix.path)
    }

    fun endsWith(suffix: SourcePath): Boolean {
        return path.endsWith(suffix.path)
    }

    companion object {
        const val sep: String = ">"
    }
}

class SetGenerateContext(
        val values: List<SqlTypeCommon>,
        val sources: Map<SourcePath, String>,
        val target: GenerateTarget,
        val path: SourcePath)

class ValueGenerateContext(
        val sources: Map<SourcePath, String>,
        val path: SourcePath)

interface SqlSet {
    fun push(constructor: SourceReference)
}

interface SetRef {
    val set: SqlSet
    fun reference(constructor: ReferenceConstructor)
}

class DummyRef(override val set: SqlSet): SetRef {
    override fun reference(constructor: ReferenceConstructor) {
    }
}

class ColumnRef(override val set: SqlSet, private val name: String): SetRef {
    override fun reference(constructor: ReferenceConstructor) {
        constructor.column(name)
    }
}

class JoinLeftRef(override val set: SqlSet, private val source: SetRef): SetRef {
    override fun reference(constructor: ReferenceConstructor) {
        source.reference(constructor.joinLeft())
    }
}

class JoinRightRef(override val set: SqlSet, private val source: SetRef): SetRef {
    override fun reference(constructor: ReferenceConstructor) {
        source.reference(constructor.joinRight())
    }
}

interface SqlTypeCommon {
    val reference: Collection<SetRef>
    fun push(constructor: ExpressionConstructor)
}

interface SqlType<MAPPING_TYPE>: SqlTypeCommon {
    fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): MAPPING_TYPE?
}