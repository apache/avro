package demo

import example.avro.User

fun main(args : Array<String>) {
    val user = User("David", 24, "blue", null)
    println("Hello, ${user.getName()}")
}
