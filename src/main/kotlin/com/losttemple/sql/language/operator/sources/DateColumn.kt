package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.operator.SqlDialect
import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.types.SetRef
import java.sql.ResultSet
import java.sql.Types
import java.util.*

class DateColumn(setReference: SetRef,
                          name: String):
        CommonColumn<Date, DateColumn>(setReference, name) {
    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(
                    Types.DATE,
                    isNullable = isNullable,
                    isSearchable = isSearchable)
        }

    override fun generateParameter(parameter: Date?): SqlParameter {
        val sqlDate = if (parameter == null) null else java.sql.Date(parameter.time)
        return object: SqlParameter {
            override fun setParam(constructor: ExpressionConstructor) {
                constructor.constance(sqlDate)
            }
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Date? {
        return dialect.dateResult(result, name)
    }
}

class TimeColumn(setReference: SetRef,
                 name: String):
        CommonColumn<Date, DateColumn>(setReference, name) {
    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(
                    Types.TIME,
                    isNullable = isNullable,
                    isSearchable = isSearchable)
        }

    override fun generateParameter(parameter: Date?): SqlParameter {
        val sqlDate = if (parameter == null) null else java.sql.Time(parameter.time)
        return object: SqlParameter {
            override fun setParam(constructor: ExpressionConstructor) {
                constructor.constance(sqlDate)
            }
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Date? {
        return dialect.timeResult(result, name)
    }
}

class TimestampColumn(setReference: SetRef,
                 name: String):
        CommonColumn<Date, DateColumn>(setReference, name) {
    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(
                    Types.TIMESTAMP,
                    isNullable = isNullable,
                    isSearchable = isSearchable)
        }

    override fun generateParameter(parameter: Date?): SqlParameter {
        val sqlDate = if (parameter == null) null else java.sql.Timestamp(parameter.time)
        return object: SqlParameter {
            override fun setParam(constructor: ExpressionConstructor) {
                constructor.constance(sqlDate)
            }
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Date? {
        return dialect.timestampResult(result, name)
    }
}