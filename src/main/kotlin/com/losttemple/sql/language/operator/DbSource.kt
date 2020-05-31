package com.losttemple.sql.language.operator

import com.losttemple.sql.language.dialects.JdbcIntParameter
import com.losttemple.sql.language.dialects.JdbcSqlParameter
import com.losttemple.sql.language.dialects.JdbcStringParameter
import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.generate.ReferenceConstructor
import com.losttemple.sql.language.generate.SourceReference
import com.losttemple.sql.language.operator.sources.*
import com.losttemple.sql.language.types.*
import java.math.BigInteger
import java.sql.Timestamp
import java.sql.Types
import java.util.*


interface SetPrefix {
    fun fillPath(source: SetRef): SetRef
    fun append(op: (ReferenceConstructor) -> Unit): SetPrefix
}

class MergedSetRef(
        override val set: SqlSet,
        private val operators: List<(ReferenceConstructor) -> Unit>,
        private val end: SetRef): SetRef {
    override fun reference(constructor: ReferenceConstructor) {
        for (operator in operators) {
            operator(constructor)
        }
        end.reference(constructor)
    }
}

class OperatorPrefix(private val set: SqlSet, private val operators: List<(ReferenceConstructor) -> Unit>): SetPrefix {
    override fun fillPath(source: SetRef): SetRef {
        return MergedSetRef(set, operators, source)
    }

    override fun append(op: (ReferenceConstructor) -> Unit): SetPrefix {
        return OperatorPrefix(set, operators + listOf(op))
    }
}

class SetReplace(override val set: SqlSet, private val end: SetRef): SetRef {
    override fun reference(constructor: ReferenceConstructor) {
        end.reference(constructor)
    }
}

class DummyPrefix(private val set: SqlSet): SetPrefix {
    override fun fillPath(source: SetRef): SetRef {
        return SetReplace(set, source)
    }

    override fun append(op: (ReferenceConstructor) -> Unit): SetPrefix {
        return OperatorPrefix(set, listOf(op))
    }
}

interface DbValue<T> {
    val description: T
    val set: SqlSet
    fun wrapDescription(prefix: SetPrefix): T
}

interface DbSet<T>: DbValue<T> {
}

interface DbInstance<T>: DbValue<T> {
    val value: T
        get() {
            val prefix = DummyPrefix(set)
            return wrapDescription(prefix)
        }
}

interface SqlParameter {
    fun setParam(constructor: ExpressionConstructor)
}

class SqlTypeParameter<T>(private val value: SqlType<T>): SqlParameter {
    override fun setParam(constructor: ExpressionConstructor) {
        value.push(constructor)
    }
}

class IntSqlParameter(private val value: BigInteger?): SqlParameter {
    override fun setParam(constructor: ExpressionConstructor) {
        constructor.constance(value)
    }
}

class StringSqlParameter(private val value: String?): SqlParameter {
    override fun setParam(constructor: ExpressionConstructor) {
        constructor.constance(value)
    }
}

class DateSqlParameter(private val value: Date?): SqlParameter {
    override fun setParam(constructor: ExpressionConstructor) {
        constructor.constance(if (value == null) value else Timestamp(value.time))
    }
}

class BoolSqlParameter(private val value: Boolean?): SqlParameter {
    override fun setParam(constructor: ExpressionConstructor) {
        constructor.constance(value)
    }
}

class DoubleSqlParameter(private val value: Double?): SqlParameter {
    override fun setParam(constructor: ExpressionConstructor) {
        constructor.constance(value)
    }
}

class IntSourceColumn(private val setReference: SetRef, override val name: String): SqlInt, SourceColumn<Int> {
    override val reference: Collection<SetRef>
        get() = listOf(setReference)

    override fun push(constructor: ExpressionConstructor) {
        val containSource = constructor.reference({ setReference.set.push(it) }) {
            setReference.reference(it)
            it.column(name)}
        if (!containSource) {
            val query = constructor.embedQuery()
            setReference.set.push(query)
            val selector = query.addSelector()
            push(selector)
        }
    }

    override fun generateParameter(parameter: Int?): SqlParameter {
        return IntSqlParameter(parameter?.toBigInteger())
    }
}

class StringSourceColumn(private val setReference: SetRef, override val name: String): SqlString, SourceColumn<String> {
    override val reference: Collection<SetRef>
        get() = listOf(setReference)

    override fun push(constructor: ExpressionConstructor) {
        val containSource = constructor.reference({ setReference.set.push(it) }) {
            setReference.reference(it)
            it.column(name)}
        if (!containSource) {
            val query = constructor.embedQuery()
            setReference.set.push(query)
            val selector = query.addSelector()
            push(selector)
        }
    }

    override fun generateParameter(parameter: String?): SqlParameter {
        return StringSqlParameter(parameter)
    }
}

class DateSourceColumn(private val setReference: SetRef, override val name: String): SqlDate, SourceColumn<Date> {
    override val reference: Collection<SetRef>
        get() = listOf(setReference)

    override fun push(constructor: ExpressionConstructor) {
        val containSource = constructor.reference({ setReference.set.push(it) }) {
            setReference.reference(it)
            it.column(name)}
        if (!containSource) {
            val query = constructor.embedQuery()
            setReference.set.push(query)
            val selector = query.addSelector()
            push(selector)
        }
    }

    override fun generateParameter(parameter: Date?): SqlParameter {
        return DateSqlParameter(parameter)
    }
}

class BoolSourceColumn(private val setReference: SetRef, override val name: String): SqlBool, SourceColumn<Boolean> {
    override val reference: Collection<SetRef>
        get() = listOf(setReference)

    override fun push(constructor: ExpressionConstructor) {
        val containSource = constructor.reference({ setReference.set.push(it) }) {
            setReference.reference(it)
            it.column(name)}
        if (!containSource) {
            val query = constructor.embedQuery()
            setReference.set.push(query)
            val selector = query.addSelector()
            push(selector)
        }
    }

    override fun generateParameter(parameter: Boolean?): SqlParameter {
        return BoolSqlParameter(parameter)
    }
}

class DoubleSourceColumn(private val setReference: SetRef, override val name: String): SqlDouble, SourceColumn<Double> {
    override val reference: Collection<SetRef>
        get() = listOf(setReference)

    override fun push(constructor: ExpressionConstructor) {
        val containSource = constructor.reference({ setReference.set.push(it) }) {
            setReference.reference(it)
            it.column(name)}
        if (!containSource) {
            val query = constructor.embedQuery()
            setReference.set.push(query)
            val selector = query.addSelector()
            push(selector)
        }
    }

    override fun generateParameter(parameter: Double?): SqlParameter {
        return DoubleSqlParameter(parameter)
    }
}

open class  DbSource(val reference: SetRef) {
    fun tinyIntColumn(name: String): ShortColumn {
        return ShortColumn(reference, name, Types.TINYINT, 8)
    }

    fun smallIntColumn(name: String): ShortColumn {
        return ShortColumn(reference, name, Types.SMALLINT, 16)
    }

    fun intColumn(name: String): IntColumn {
        return IntColumn(reference, name, Types.INTEGER, 32)
    }

    fun bigIntColumn(name: String): LongColumn {
        return LongColumn(reference, name, Types.BIGINT, 64)
    }

    fun charColumn(name: String): StringColumn {
        return StringColumn(reference, name, Types.CHAR)
    }

    fun varCharColumn(name: String): StringColumn {
        return StringColumn(reference, name, Types.VARCHAR)
    }

    fun longVarCharColumn(name: String): StringColumn {
        return StringColumn(reference, name, Types.LONGNVARCHAR)
    }

    fun nCharColumn(name: String): StringColumn {
        return StringColumn(reference, name, Types.NCHAR)
    }

    fun varNCharColumn(name: String): StringColumn {
        return StringColumn(reference, name, Types.NVARCHAR)
    }

    fun longNVarCharColumn(name: String): StringColumn {
        return StringColumn(reference, name, Types.LONGNVARCHAR)
    }

    fun stringColumn(name: String): SourceColumn<String> {
        return StringSourceColumn(reference, name)
    }

    fun dateColumn(name: String): SourceColumn<Date> {
        return DateSourceColumn(reference, name)
    }

    fun boolColumn(name: String): SourceColumn<Boolean> {
        return BoolSourceColumn(reference, name)
    }

    fun doubleColumn(name: String): SourceColumn<Double> {
        return DoubleSourceColumn(reference, name)
    }
}

interface TableConfigure {
    fun tableName(name: String)
}

private class TableSet<T: DbSource>(private val sourceSet: SourceSet, override val description: T,
                  private val creator: ((TableConfigure.()->Unit)->SetRef)->T): DbSet<T> {
    override val set: SqlSet
        get() = sourceSet

    override fun wrapDescription(prefix: SetPrefix): T {
        return creator { prefix.fillPath(DummyRef(sourceSet)) }
    }
}

class SourceSet(val name: String): SqlSet {
    val reference: SetRef = DummyRef(this)

    override fun push(constructor: SourceReference) {
        constructor.from.table(name)
    }
}

class SourceTableConfigure: TableConfigure {
    private var name: String = ""

    override fun tableName(name: String) {
        this.name = name
    }

    fun toSource(): SourceSet {
        return SourceSet(name)
    }
}

fun <T: DbSource> from(creator: ((TableConfigure.()->Unit)->SetRef)->T): DbSet<T> {
    val sourceConfig = SourceTableConfigure()
    val description = creator {
        sourceConfig.it()
        sourceConfig.toSource().reference
    }
    return TableSet<T>(description.reference.set as SourceSet, description, creator)
}
/*
fun <T: DbSource> delete(machine: SqlDialect, connection: Connection, creator: ((TableConfigure.()->Unit)->SetRef)->T) {
    db(creator).delete(machine, connection)
}*/