package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.operator.BoolSqlParameter
import com.losttemple.sql.language.operator.IntSqlParameter
import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.types.SetRef
import com.losttemple.sql.language.types.SqlBool
import com.losttemple.sql.language.types.SqlInt
import java.sql.Types

abstract class CommonColumn<T, R>(private val setReference: SetRef,
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

class BitAsIntColumn(private val setReference: SetRef, name: String):
        SqlInt,
        CommonColumn<Int, BitAsIntColumn>(setReference, name) {
    override fun generateParameter(parameter: Int?): SqlParameter {
        return IntSqlParameter(parameter)
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(Types.BIT, isNullable = isNullable, isSearchable = isSearchable, precision = 1)
        }
}

class BitAsBoolColumn(private val setReference: SetRef, name: String):
        SqlBool,
        CommonColumn<Boolean, BitAsIntColumn>(setReference, name) {
    override fun generateParameter(parameter: Boolean?): SqlParameter {
        val notNull = parameter ?: return IntSqlParameter(null)
        return if (notNull) {
            IntSqlParameter(1)
        }
        else {
            IntSqlParameter(0)
        }
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(Types.BIT)
        }
}