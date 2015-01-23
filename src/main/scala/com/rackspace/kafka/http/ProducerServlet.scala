package com.rackspace.kafka.http

import scala.collection.JavaConversions._

import java.io.IOException
import java.util.Properties
import java.util.HashMap
import java.util.concurrent.TimeUnit
import java.net.URLDecoder

import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.apache.log4j.Logger;

import kafka.producer._
import kafka.message._
import kafka.serializer._
import scala.collection.mutable._

import com.mongodb.util.JSON
import com.timgroup.statsd.NonBlockingStatsDClient

import org.bson.BSON
import org.bson.BSONObject
import org.bson.BasicBSONDecoder
import org.bson.BasicBSONEncoder
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

class ProducerServlet(properties:Properties, reportingProps: Properties) extends HttpServlet with ReplyFormatter
{
  val producer = new Producer[String, String](new ProducerConfig(properties))
  val logger = Logger.getLogger("kafka.http.producer")

  var statsd = new NonBlockingStatsDClient(
    reportingProps.getProperty("statsd.prefix"),
    reportingProps.getProperty("statsd.host"),
    reportingProps.getProperty("statsd.port").toInt)

  def asList(name: String, o:AnyRef):BasicBSONList = {
    try{
      return o.asInstanceOf[BasicBSONList]
    }
    catch {
      case e: Exception => {
        throw new Exception("Expected list in %s".format(name))
      }
    }
  }

  def asObject(name:String, o:AnyRef):BSONObject = {
    try{
      return o.asInstanceOf[BSONObject]
    }
    catch {
      case e: Exception => {
        throw new Exception("Expected object in %s".format(name))
      }
    }
  }

  def asString(name:String, o:AnyRef):String = {
    try{
      return o.asInstanceOf[String]
    }
    catch {
      case e: Exception => {
        throw new Exception("Expected object in %s".format(name))
      }
    }
  }

  def normalizeValue(value:BSONObject):BSONObject = {
    var newvalue = value
    val companyId = asString("companyid", newvalue.get("CompanyId"))
    val tag = asString("tag", newvalue.get("Tag"))
    val clientVersion = asString("clientversion", newvalue.get("ClientVersion"))
    val devCode = asString("devcode", newvalue.get("DevCode"))
    val pid = asString("pid", newvalue.get("Pid"))

    if (companyId == null) {
      newvalue.put("CompanyId", "")
    }

    if (tag == null) {
      newvalue.put("Tag", "")
    }

    if (clientVersion == null) {
      newvalue.put("ClientVersion", "")
    }

    if (devCode == null) {
      newvalue.put("DevCode", "")
    }

    if (pid == null) {
      newvalue.put("Pid", "")
    }

    /*companyId match {
      case None =>  {
        val a = ""
        newvalue.put("CompanyId", a)
      }
    }
    tag match {
      case None => newvalue.put("Tag", "")
    }
    clientVersion match {
      case None => newvalue.put("ClientVersion", "")
    }
    devCode match {
      case None => newvalue.put("DevCode", "")
    }
    pid match {
      case None => newvalue.put("Pid", "")
    }*/
    newvalue
  }

  def toKeyedMessage(topic:String, o:BSONObject):KeyedMessage[String, String] = {
    var key:String = asString("'key' property", o.get("key"))
    var value = asObject("'value' property", o.get("value"))
    if(value == null) {
      throw new Exception("Expected 'value' property in message")
    }
    //return new KeyedMessage[String, Array[Byte]](topic, key, new BasicBSONEncoder().encode(value))
    //val jsonstr = JSON.serialize(new BasicBSONEncoder().encode(value))
    value = normalizeValue(value)
    val jsonstr = value.toString()
    System.out.println("######to key message:"+jsonstr)
    return new KeyedMessage[String, String](topic, key, jsonstr)
  }

  def getObject(request:HttpServletRequest):MutableList[KeyedMessage[String, String]] = {
    var topic = getTopic(request)

    val obj = request.getContentType() match {
      case "application/json" => getObjectFromJson(request)
      case "application/bson" => getObjectFromBson(request)
      case _ => throw new Exception("Unsupported content type: %s".format(request.getContentType()))
    }
    if(obj == null) {
      throw new Exception("Provide a payload for the POST request")
    }

    var messagesI = obj.get("messages")
    if(messagesI == null) {
      throw new Exception("Expected 'messages' list")
    }
    var messages = asList("'messages' parameter", messagesI)
    var list = new MutableList[KeyedMessage[String, String]]()

    for (messageI <- messages) {
      var message = asObject("message", messageI)
      list += toKeyedMessage(topic, message)
    }
    list
  }

  def getObjectForGet(request:HttpServletRequest):MutableList[KeyedMessage[String, String]] = {
    var topic = getTopicForGet(request)
    val obj = request.getContentType() match {
      case "application/json" => getObjectFromJsonForGet(request)
      case _ => throw new Exception("Unsupported content type: %s".format(request.getContentType()))
    }
   
    if(obj == null) {
      throw new Exception("Provide a payload for the GET request")
    }

    var messagesI = obj.get("messages")
    if(messagesI == null) {
      throw new Exception("Expected 'messages' list")
    }
    var messages = asList("'messages' parameter", messagesI)
    var list = new MutableList[KeyedMessage[String, String]]()

    for (messageI <- messages) {
      var message = asObject("message", messageI)
      list += toKeyedMessage(topic, message)
    }
    list

  }

  def getObjectForGetSingleMessage(request:HttpServletRequest):MutableList[KeyedMessage[String, String]] = {
    var topic = getTopicForGet(request)
    var obj = request.getContentType() match {
      case "application/json" => getObjectFromJsonForGet(request)
      case _ => throw new Exception("Unsupported content type: %s".format(request.getContentType()))
    }
    var list = new MutableList[KeyedMessage[String, String]]()
    obj = normalizeValue(obj)
    val jsonstr = obj.toString()
    System.out.println("######to key message in single:"+jsonstr)
    val singlemessage = new KeyedMessage[String, String](topic, "", jsonstr)
    list += singlemessage
    list
  }

  def getObjectForPostSingleMessage(request:HttpServletRequest):MutableList[KeyedMessage[String, String]] = {
    var topic = getTopicForPost(request)
    var obj = request.getContentType() match {
      case "application/json" => getObjectFromJsonForPost(request)
      case _ => throw new Exception("Unsupported content type: %s".format(request.getContentType()))
    }
    var list = new MutableList[KeyedMessage[String, String]]()
    obj = normalizeValue(obj)
    val jsonstr = obj.toString()
    System.out.println("######to key message in single:"+jsonstr)
    val singlemessage = new KeyedMessage[String, String](topic, "", jsonstr)
    list += singlemessage
    list
  }

  def getObjectFromBson(request:HttpServletRequest):BSONObject = {
    return new BasicBSONDecoder().readObject(request.getInputStream())
  }

  def getObjectFromJson(request:HttpServletRequest):BSONObject = {
    var body = new StringBuilder
    var reader = request.getReader()
    var buffer = new Array[Char](4096)
    var len:Int = 0

    while ({len = reader.read(buffer, 0, buffer.length); len != -1}) {
      body.appendAll(buffer, 0, len);
    }
    System.out.println("#####body string:"+body.toString())
    return JSON.parse(body.toString()).asInstanceOf[BSONObject]
  }

  def getObjectFromJsonForPost(request:HttpServletRequest):BSONObject = {
    var body = new StringBuilder
    var reader = request.getReader()
    var buffer = new Array[Char](4096)
    var len:Int = 0

    while ({len = reader.read(buffer, 0, buffer.length); len != -1}) {
      body.appendAll(buffer, 0, len);
    }
    System.out.println("#####body string:"+body.toString())
    return JSON.parse(body.toString()).asInstanceOf[BSONObject]
  }
 
  def getObjectFromJsonForGet(request:HttpServletRequest):BSONObject = {
    /*val segments = request.getRequestURI().split("/")
    val messagesstr = URLDecoder.decode(segments(3))*/
    val messagesstr = URLDecoder.decode(request.getParameter("data"))
    System.out.println("#####message decode str:" + messagesstr)
    return JSON.parse(messagesstr).asInstanceOf[BSONObject]
  }

  def getTopic(request:HttpServletRequest):String = {
    var segments = request.getRequestURI().split("/")
    if (segments.length != 3 || segments(1) != "topics") {
      throw new Exception("Please provide topic /topics/<topic> to post to")
    } 
    return segments(2)
  }

  def getTopicForGet(request:HttpServletRequest):String = {
    /*var segments = request.getRequestURI().split("/")
    if (segments.length != 4 || segments(1) != "topics") {
      throw new Exception("Please provide topic and messages /topics/<topic>/<messages> to get method")
    }*/

    val topic = request.getParameter("topic")
    if (topic == null) {
      throw new Exception("Please provide topic in query string")
    }
    return topic
  }

  def getTopicForPost(request:HttpServletRequest):String = {
    val topic = request.getParameter("topic")
    System.out.println("####post topic:"+topic)
    if (topic == null) {
      throw new Exception("Please provide topic in query string")
    }
    return topic
  }

  override def doPost(request:HttpServletRequest, response:HttpServletResponse)
  {
   try{ 
    var messages = new MutableList[KeyedMessage[String, String]]()
    if (request.getParameter("type") == "batch") {
      messages = getObject(request)
    } else {
      messages = getObjectForPostSingleMessage(request)
    }

    val start = System.currentTimeMillis()
    producer.send(messages:_*)
    statsd.recordExecutionTime("submitted", (System.currentTimeMillis() - start).toInt)

    var obj = new BasicBSONObject()
    obj.append("accepted", "OK")
    logger.info("""{"accepted": "OK"}""")
    replyWith(obj, request, response)
   } catch {
     case e: Exception => {
       logger.info("""{"error":""" + "\""+e.toString + "\"}")
       throw e       
     }
   }
  }
  
  override def doGet(request:HttpServletRequest, response:HttpServletResponse)
  {
    System.out.println("####process get method")
    System.out.println("####pos a:"+request.getQueryString())
    val parmap = request.getParameterMap()
    for ((k,v) <- parmap) printf("#####key: %s, value: %s\n", k, v)

    //var topic = getTopicForGet(request)
    var messages = new MutableList[KeyedMessage[String, String]]()
    if (request.getParameter("type") == "batch") {
      messages = getObjectForGet(request)
    } else {
      messages = getObjectForGetSingleMessage(request)
    }

    val start = System.currentTimeMillis()
    //val data = new KeyedMessage[String, Array[Byte]](topic, "key", new Array[Byte](1))
    producer.send(messages:_*)
    statsd.recordExecutionTime("submitted", (System.currentTimeMillis() - start).toInt)

    var obj = new BasicBSONObject()
    obj.append("accepted", "OK")
    replyWith(obj, request, response)
    
  }
}
