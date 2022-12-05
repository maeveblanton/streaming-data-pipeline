package com.labs1904.hwe.model

case class EnrichedUser(
                         id: String,
                         numberAsWord: String,
                         username: String,
                         name: String,
                         gender: Char,
                         email: String,
                         dob: String,
                         hweDeveloper: String
                       ) {
  override def toString: String = {
    s"$id,$numberAsWord,$username,$name,$gender,$email,$dob,$hweDeveloper"
  }
}
