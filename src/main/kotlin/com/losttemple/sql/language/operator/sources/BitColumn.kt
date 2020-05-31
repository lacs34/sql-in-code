package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.operator.BoolSqlParameter
import com.losttemple.sql.language.operator.IntSqlParameter
import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.types.SetRef
import com.losttemple.sql.language.types.SqlBool
import com.losttemple.sql.language.types.SqlInt
import java.math.BigInteger
import java.sql.Types

abstract class CommonColumn<T, R>(protected val setReference: SetRef,
                               override val name: String): SqlSourceColumn<T> {
    protected var isNullable: Boolean = false
        private set

    protected var isSearchable: Boolean = true
        private set

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

    fun toActual(): R {
        @Suppress("UNCHECKED_CAST")
        return this as R
    }

    fun nullable(): R {
        isNullable = true
        return toActual()
    }

    fun notSearchable(): R {
        isSearchable = false
        return toActual()
    }
}

class BitAsIntColumn(setReference: SetRef, name: String):
        SqlInt,
        CommonColumn<Int, BitAsIntColumn>(setReference, name) {
    override fun generateParameter(parameter: Int?): SqlParameter {
        return IntSqlParameter(parameter?.toBigInteger())
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(Types.BIT, isNullable = isNullable, isSearchable = isSearchable, precision = 1)
        }
}

class BitAsBoolColumn(setReference: SetRef, name: String):
        SqlBool,
        CommonColumn<Boolean, BitAsIntColumn>(setReference, name) {
    override fun generateParameter(parameter: Boolean?): SqlParameter {
        val notNull = parameter ?: return IntSqlParameter(null)
        return if (notNull) {
            IntSqlParameter(1.toBigInteger())
        }
        else {
            IntSqlParameter(0.toBigInteger())
        }
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(Types.BIT)
        }
}