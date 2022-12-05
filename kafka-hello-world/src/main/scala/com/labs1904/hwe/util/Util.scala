package com.labs1904.hwe.util

object Util {
  def getScramAuthString(username: String, password: String) = {
   s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }

  def mapNumberToWord(number: String) : String = {
    number.map(numberToWordMap)
  }

  val numberToWordMap: Map[Char, Char] = Map[Char, Char](
    '1' -> 'a'
    ,'2' -> 'b'
    ,'3' -> 'c'
    ,'4' -> 'd'
    ,'5' -> 'e'
    ,'6' -> 'f'
    ,'7' -> 'g'
    ,'8' -> 'h'
    ,'9' -> 'i'
    ,'0' -> 'j'
  )
}
