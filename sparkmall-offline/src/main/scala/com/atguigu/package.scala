package com

package object atguigu {

  def isEmpty(s:String):Boolean = s == null || s == ""
  def isNotEmpty(s:String) = !isEmpty(s)

}
