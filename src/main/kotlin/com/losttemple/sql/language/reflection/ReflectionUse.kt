package com.losttemple.sql.language.reflection

import com.losttemple.sql.language.operator.*
import com.losttemple.sql.language.operator.sources.SourceColumn
import com.losttemple.sql.language.types.SqlTypeCommon
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
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

data class PropertyAssign<SC, TC: DbSource>(
        val sourceProperty: KProperty<*>,
        val sourceValue: Any?,
        val sourceObject: SC,
        val targetProperty: KProperty<SourceColumn<*>>,
        val targetValue: SourceColumn<*>,
        val targetObject: TC
)

fun <T: Any, S: DbSource> DbTableDescription<S>.insertObjectWithPropertyFilter(
        obj: T,
        propertyFilter: (PropertyAssign<T, S>) -> Boolean): Inserter<S> {
    val set = source.reference.set as SourceSet
    val environment = DbInsertionEnvironment(set.name)

    val sourceClass = source.javaClass.kotlin
    val sourceProperties = sourceClass.declaredMemberProperties.associateBy { it.name }
    val objClass = obj.javaClass.kotlin
    objClass.declaredMemberProperties.forEach {
        val propertyName = it.name
        val relatedSourceProperty = sourceProperties[propertyName]
        if (relatedSourceProperty != null) {
            val sourceProperty = relatedSourceProperty.get(source) as? SourceColumn<*>?
            if (sourceProperty != null) {
                val objProperty = it.get(obj)
                @Suppress("UNCHECKED_CAST")
                val assign = PropertyAssign<T, S>(
                        it,
                        objProperty,
                        obj,
                        relatedSourceProperty as KProperty1<S, SourceColumn<*>>,
                        sourceProperty,
                        source)
                if (propertyFilter(assign))
                environment.apply {
                    @Suppress("UNCHECKED_CAST")
                    (sourceProperty as SourceColumn<Any?>)(objProperty)
                }
            }
        }
    }
    return Inserter<S>(environment, source)
}

fun <T: Any, S: DbSource> DbTableDescription<S>.insertObject(
        obj: T): Inserter<S> {
    return insertObjectWithPropertyFilter(obj) { _ ->
        true
    }
}

fun <T: Any, S: DbSource> DbTableDescription<S>.insertObjectNotNull(
        obj: T): Inserter<S> {
    return insertObjectWithPropertyFilter(obj) { assign ->
        assign.sourceValue != null
    }
}

data class RetPropertyAssign<SC: DbSource, TC>(
        val sourceProperty: KProperty<SourceColumn<*>>,
        val sourceValue: SourceColumn<*>,
        val sourceObject: SC,
        val targetProperty: KProperty<*>,
        val oldTargetValue: Any?,
        val newTargetValue: Any?,
        val targetObject: TC
)

inline fun <T> Iterable<KProperty1<T, *>>.associateColumn(source: T): Map<String, KProperty1<T, *>> {
    val destination = LinkedHashMap<String, KProperty1<T, *>>()
    for (element in this) {
        val value = element.get(source)
        val column = value as? SourceColumn<*>
        if (column != null) {
            destination[column.name] = element
        }
    }
    return destination
}

fun <S: DbSource, R: Any> Inserter<S>.retWithPropertyFilter(
        target: R,
        propertyFilter: (RetPropertyAssign<S, R>) -> Boolean): InserterWithRet<S, R> {
    val source = descriptor
    val sourceClass = source.javaClass.kotlin
    val sourceProperties =
            sourceClass.declaredMemberProperties.associateColumn(source)
    val targetClass = target.javaClass.kotlin
    val targetProperties =
            targetClass.declaredMemberProperties.associateBy { it.name }
    return ret { valueSource ->
        val resultSet = result
        val resultMetaData = resultSet.metaData
        for (i in 1 .. resultMetaData.columnCount) {
            val columnName = resultMetaData.getColumnName(i).toLowerCase()
            val sourceProperty = sourceProperties[columnName]
            if (sourceProperty != null) {
                val fieldName = sourceProperty.name
                val sourceValue = sourceProperty.get(source) as? SourceColumn<*>
                val targetProperty = targetProperties[fieldName] as? KMutableProperty1<R, *>
                if (targetProperty != null) {
                    val oldTargetValue = targetProperty.get(target)
                    @Suppress("UNCHECKED_CAST")
                    val targetValue = get(sourceValue as SourceColumn<Any>)
                    @Suppress("UNCHECKED_CAST")
                    val assign: RetPropertyAssign<S, R> = RetPropertyAssign(
                            sourceProperty as KProperty<SourceColumn<*>>,
                            sourceValue,
                            source,
                            targetProperty,
                            oldTargetValue,
                            targetValue,
                            target)
                    if (propertyFilter(assign)) {
                        @Suppress("UNCHECKED_CAST")
                        (targetProperty as KMutableProperty1<R, Any?>).set(target, targetValue)
                    }
                }
            }
        }
        target
    }
}

fun <S: DbSource, R: Any> Inserter<S>.retObject(
        target: R): InserterWithRet<S, R> {
    return retWithPropertyFilter(target) { _ ->
        true
    }
}

fun <T: Any, S: DbSource> DbTableDescription<S>.updateObjectWithPropertyFilter(
        obj: T,
        propertyFilter: (PropertyAssign<T, S>) -> Boolean): Updater {
    val set = source.reference.set as SourceSet
    val environment = DbUpdateEnvironment(set.name, null)

    val sourceClass = source.javaClass.kotlin
    val sourceProperties = sourceClass.declaredMemberProperties.associateBy { it.name }
    val objClass = obj.javaClass.kotlin
    objClass.declaredMemberProperties.forEach {
        val propertyName = it.name
        val relatedSourceProperty = sourceProperties[propertyName]
        if (relatedSourceProperty != null) {
            val sourceProperty = relatedSourceProperty.get(source) as? SourceColumn<*>?
            if (sourceProperty != null) {
                val objProperty = it.get(obj)
                @Suppress("UNCHECKED_CAST")
                val assign = PropertyAssign<T, S>(
                        it,
                        objProperty,
                        obj,
                        relatedSourceProperty as KProperty1<S, SourceColumn<*>>,
                        sourceProperty,
                        source)
                if (propertyFilter(assign))
                    environment.apply {
                        @Suppress("UNCHECKED_CAST")
                        (sourceProperty as SourceColumn<Any?>)(objProperty)
                    }
            }
        }
    }
    return Updater(environment)
}

fun <T: Any, S: DbSource> DbTableDescription<S>.updateObject(
        obj: T): Updater {
    return updateObjectWithPropertyFilter(obj) { _ ->
        true
    }
}


fun <T: Any, S: DbSource> FilteredTableDescriptor<S>.updateObjectWithPropertyFilter(
        obj: T,
        propertyFilter: (PropertyAssign<T, S>) -> Boolean): Updater {
    val set = description.reference.set as SourceSet
    val environment = DbUpdateEnvironment(set.name, null)

    val sourceClass = description.javaClass.kotlin
    val sourceProperties = sourceClass.declaredMemberProperties.associateBy { it.name }
    val objClass = obj.javaClass.kotlin
    objClass.declaredMemberProperties.forEach {
        val propertyName = it.name
        val relatedSourceProperty = sourceProperties[propertyName]
        if (relatedSourceProperty != null) {
            val sourceProperty = relatedSourceProperty.get(description) as? SourceColumn<*>?
            if (sourceProperty != null) {
                val objProperty = it.get(obj)
                @Suppress("UNCHECKED_CAST")
                val assign = PropertyAssign<T, S>(
                        it,
                        objProperty,
                        obj,
                        relatedSourceProperty as KProperty1<S, SourceColumn<*>>,
                        sourceProperty,
                        description)
                if (propertyFilter(assign))
                    environment.apply {
                        @Suppress("UNCHECKED_CAST")
                        (sourceProperty as SourceColumn<Any?>)(objProperty)
                    }
            }
        }
    }
    return Updater(environment)
}

fun <T: Any, S: DbSource> FilteredTableDescriptor<S>.updateObject(
        obj: T): Updater {
    return updateObjectWithPropertyFilter(obj) { _ ->
        true
    }
}