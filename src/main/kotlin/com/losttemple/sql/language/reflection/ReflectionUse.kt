package com.losttemple.sql.language.reflection

import com.losttemple.sql.language.operator.*
import com.losttemple.sql.language.types.SqlTypeCommon
import kotlin.reflect.full.createType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.isSubtypeOf

fun <T: Any> DbSet<T>.useAll(): DbResult<T> {
    val description = description
    val descClass = description.javaClass.kotlin
    val properties = descClass.declaredMemberProperties.filter {
        it.returnType.isSubtypeOf(SqlTypeCommon::class.createType())
    }
    return this.use { entity ->
        properties.forEach {
            need(it.get(entity) as SqlTypeCommon)
        }
    }
}

fun <T: Any> DbInstance<T>.useAll(): DbInstanceResult<T> {
    val description = description
    val descClass = description.javaClass.kotlin
    val properties = descClass.declaredMemberProperties.filter {
        it.returnType.isSubtypeOf(SqlTypeCommon::class.createType())
    }
    return this.use { entity ->
        properties.forEach {
            need(it.get(entity) as SqlTypeCommon)
        }
    }
}