package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.operator.IntSqlParameter
import com.losttemple.sql.language.operator.SqlDialect
import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.types.SetRef
import com.losttemple.sql.language.types.SqlInt
import java.math.BigInteger
import java.sql.ResultSet
import java.sql.Types

abstract class NumberColumn<T, R>(setReference: SetRef,
                                  name: String,
                                  protected val type: Int,
                                  protected var precision: Int):
        CommonColumn<T, R>(setReference, name) {
    private var isAutoIncrement: Boolean = false
    private var isSigned: Boolean = true

    protected fun <TA, RA> copyTo(another: NumberColumn<TA, RA>) {
        another.isAutoIncrement = isAutoIncrement
        another.isSigned = isSigned
    }

    fun autoIncrement(): R {
        isAutoIncrement = true
        return toActual()
    }

    fun unsigned(): R {
        isSigned = false
        return toActual()
    }

    fun advisePrecision(precision: Int): R {
        this.precision = precision
        return toActual()
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(
                    type,
                    isAutoIncrement = isAutoIncrement,
                    isNullable = isNullable,
                    isSearchable = isSearchable,
                    isSigned = isSigned,
                    precision = precision)
        }
}

class BigIntegerColumn(setReference: SetRef,
                       name: String,
                       type: Int,
                       precision: Int):
        NumberColumn<BigInteger, BigIntegerColumn>(setReference, name, type, precision) {
    override fun generateParameter(parameter: BigInteger?): SqlParameter {
        return IntSqlParameter(parameter)
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): BigInteger? {
        return dialect.bigIntResult(result, name)
    }

    fun asByte(): ByteColumn {
        val column = ByteColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asShort(): ShortColumn {
        val column = ShortColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asInt(): IntColumn {
        val column = IntColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asLong(): LongColumn {
        val column = LongColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }
}

class LongColumn(setReference: SetRef,
                 name: String,
                 type: Int,
                 precision: Int):
        NumberColumn<Long, LongColumn>(setReference, name, type, precision) {
    override fun generateParameter(parameter: Long?): SqlParameter {
        return IntSqlParameter(parameter?.toBigInteger())
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Long? {
        return dialect.longResult(result, name)
    }

    fun asByte(): ByteColumn {
        val column = ByteColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asShort(): ShortColumn {
        val column = ShortColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asInt(): IntColumn {
        val column = IntColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asBigInteger(): BigIntegerColumn {
        val column = BigIntegerColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }
}

class IntColumn(setReference: SetRef,
                name: String,
                type: Int,
                precision: Int):
        NumberColumn<Int, IntColumn>(setReference, name, type, precision),
        SqlInt {
    override fun generateParameter(parameter: Int?): SqlParameter {
        return IntSqlParameter(parameter?.toBigInteger())
    }

    fun asByte(): ByteColumn {
        val column = ByteColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asShort(): ShortColumn {
        val column = ShortColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asLong(): LongColumn {
        val column = LongColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asBigInteger(): BigIntegerColumn {
        val column = BigIntegerColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }
}

class ShortColumn(setReference: SetRef,
                  name: String,
                  type: Int,
                  precision: Int):
        NumberColumn<Short, ShortColumn>(setReference, name, type, precision) {
    override fun generateParameter(parameter: Short?): SqlParameter {
        return IntSqlParameter(parameter?.toInt()?.toBigInteger())
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Short? {
        return dialect.shortResult(result, name)
    }

    fun asByte(): ByteColumn {
        val column = ByteColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asInt(): IntColumn {
        val column = IntColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asLong(): LongColumn {
        val column = LongColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asBigInteger(): BigIntegerColumn {
        val column = BigIntegerColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }
}

class ByteColumn(setReference: SetRef,
                 name: String,
                 type: Int,
                 precision: Int):
        NumberColumn<Byte, ByteColumn>(setReference, name, type, precision) {
    override fun generateParameter(parameter: Byte?): SqlParameter {
        return IntSqlParameter(parameter?.toInt()?.toBigInteger())
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Byte? {
        return dialect.byteResult(result, name)
    }

    fun asShort(): ShortColumn {
        val column = ShortColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asInt(): IntColumn {
        val column = IntColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asLong(): LongColumn {
        val column = LongColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }

    fun asBigInteger(): BigIntegerColumn {
        val column = BigIntegerColumn(setReference, name, type, precision)
        copyTo(column)
        return column
    }
}