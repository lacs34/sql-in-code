package com.losttemple.sql.language.operator
/*
import com.losttemple.testdb.SrcDevice
import com.losttemple.sql.language.dialects.connectMySql
import com.losttemple.sql.language.reflection.useAll
import com.losttemple.sql.language.types.SetRef
import com.losttemple.sql.language.types.SqlInt
import com.losttemple.sql.language.types.SqlString
import com.losttemple.testdb.SrcRecord

class User(set: (TableConfigure.()->Unit)->SetRef) :
        DbSource(set { tableName("User") }) {
    var id = intColumn("id")
    var name = stringColumn("name")
    var password = stringColumn("password")
    var logo = intColumn("logo")
}

class Logo(set: (TableConfigure.()->Unit)->SetRef) :
        DbSource(set { tableName("Logo") }) {
    var id = intColumn("id")
    var name = stringColumn("name")
}

class Item(set: (TableConfigure.()->Unit)->SetRef) :
        DbSource(set { tableName("Item") }) {
    var id = intColumn("id")
    var buyer = intColumn("buyer")
    var name = stringColumn("name")
}

class RecordWithDevice(
        val record: SrcRecord,
        val device: SrcDevice
)

fun recordWithDevice(): DbSet<RecordWithDevice> {
    return from { SrcDevice(it) } innerJoin from { SrcRecord(it) } on {device, record ->
        device.id eq record.device
    } select {device, record ->
        RecordWithDevice(record, device)
    }
}
fun x() {
    connectMySql("jdbc:mysql://localhost:3306/water?serverTimezone=UTC", "root", "6238562a") {
        /**/
        val sql = from { SrcDevice(it) }.group({ min }, {max}).select {
            object {
                val min = keys()[0]
                val max = max { modifyTime }
                val mt = count { id }
            }
        }.useAll().select {
            object {
                val x = it.min()
                val y = it.max()
                val z = it.mt()
            }
        }
        println(sql)
    }
}*/