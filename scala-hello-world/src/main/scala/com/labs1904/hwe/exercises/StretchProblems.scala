package com.labs1904.hwe.exercises

import scala.annotation.tailrec

object StretchProblems {

  /*
  Checks if a string is palindrome.
 */
  def isPalindrome(s: String): Boolean = {
    s == s.reverse
  }

  /*
For a given number, return the next largest number that can be created by rearranging that number's digits.
If no larger number can be created, return -1
 */
  def getNextBiggestNumber(i: Integer): Int = {
    val str = i.toString
    if (str.sorted.reverse == str) -1
    else {

      @tailrec
      def recurse(n: String, ending: String = ""): String = {
        if (n.last > n.init.last) {
          n.init.init + n.last + (n.init.last + ending).sorted
        } else {
          recurse(n.init, n.last + ending)
        }
      }
      recurse(str).toInt
    }
  }

}
