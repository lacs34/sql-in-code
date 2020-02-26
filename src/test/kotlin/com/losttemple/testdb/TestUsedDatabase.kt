package com.losttemple.testdb

import com.losttemple.sql.language.operator.DbSource
import com.losttemple.sql.language.operator.TableConfigure
import com.losttemple.sql.language.types.SetRef
import java.text.DateFormat
import java.util.*

class Rooms(set: (TableConfigure.()->Unit)-> SetRef) :
        DbSource(set { tableName("rooms") }) {
    var id = intColumn("id")
    var apartment = intColumn("apartment")
    var type = intColumn("type")
    var floor = intColumn("floor")
    var sold = boolColumn("sold")
    var soldTime = dateColumn("sold_time")
    var capacity = intColumn("capacity")
}

data class Room(
        val id: Int,
        val apartment: Int,
        val type: Int,
        val floor: Int,
        val sold: Boolean,
        val soldTime: Date,
        val capacity: Int
)

val roomsData = listOf<Room>(
        Room(1,1,0,2,false, Date(2017 - 1900, 1 - 1, 5, 13, 24, 52),20),
        Room(2,1,0,3,false, Date(2017 - 1900, 8 - 1, 12, 17, 2, 33),20),
        Room(3,1,0,4,false, Date(2017 - 1900, 12 - 1, 4, 9, 47, 12),20),
        Room(4,1,0,5,false, Date(2018 - 1900, 4 - 1, 30, 15, 4, 55),20),
        Room(5,1,0,7,false, Date(2020 - 1900, 2 - 1, 4, 17, 19, 49),20)
)

class Apartments(set: (TableConfigure.()->Unit)-> SetRef) :
        DbSource(set { tableName("apartments") }) {
    var id = intColumn("id")
    var address = stringColumn("address")
    var lat = doubleColumn("lat")
    var lon = doubleColumn("lon")
    var buildTime = dateColumn("build_time")
    var maintenanceTime = dateColumn("maintenance_time")
    var buildup = boolColumn("buildup")
    var minFloor = intColumn("min_floor")
    var maxFloor = intColumn("max_floor")
}

data class Apartment(
        val id: Int,
        val address: String,
        val lat: Double,
        val lon: Double,
        val buildTime: Date,
        val maintenanceTime: Date,
        val buildup: Boolean,
        val minFloor: Int,
        val maxFloor: Int
)

val apartmentsData = listOf<Apartment>(
        Apartment(1,"园山街道保安社区简一村龙腾街25-1",22.51124,114.05122,Date(2017 - 1900, 1 - 1, 1),Date(2017 - 1900, 1 - 1, 1),true,1,8),
        Apartment(2,"松岗街道东方社区田洋二路1号B801B区",22.51233,114.05302,Date(2017 - 1900, 2 - 1, 5),Date(2017 - 1900, 7 - 1, 5),false,1,20)
)