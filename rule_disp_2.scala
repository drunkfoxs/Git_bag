import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.io

val INPUT_FILE = "datamine/outputFile/rule_8_9"
//val INPUT_DATA = "dataplatform/shanxi/input/2014_08/tfa_alarm082*"
val INPUT_DATA = "dataplatform/shanxi/input/2014_0*"
val OUTPUT_FILE = "datamine/outputFile/DISPLAY_8_9"
val CONF_FILE = "datamine/confFile/conf_disp.txt"
val LABLE_FILE = "datamine/redisDatamine/objects_20140910_utf8.unl"

val LINE_SEP = "\\ |\n"
val FIELD_SEP = "_\\|"
val SEP = "\001"
val TEXT_SEP = "\002"
val INVALID_ALA_LEN = 185

val LABLE_INT_ID_LOC = 0
val LABLE_USER_LOC = 6
val LABLE_ZH_LOC = 15

//读取告警字段位置文件
val locat = sc.textFile(CONF_FILE).map(x => (x.split("=")(0), x.split("=")(1).toInt))
val int_alm_id_loc = 0
val int_id_loc = locat.filter(x => x._1 == "INT_ID").map(_._2).toArray.toList(0) 
val alm_id_loc = locat.filter(x => x._1 == "STANDARD_ALARM_ID").map(_._2).toArray.toList(0) 
val obj_loc= locat.filter(x => x._1 == "OBJECT_CLASS").map(_._2).toArray.toList(0) 
val alm_text0_loc = locat.filter(x => x._1 == "ALARM_TEXT0").map(_._2).toArray.toList(0)
val org_loc= 18
val org_type_loc = 20 
val res_stat_loc = 40
val stad_flag_loc = 164

val field_loc = locat.map(_._2).toArray.toList
//val field_loc = List(13, 14, 18, 36, 56, 63, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 133, 134, 156, 157, 167)
val filt_loc = List(org_loc,org_type_loc, res_stat_loc, stad_flag_loc) ++ field_loc
val text_loc = locat.filter(x => x._1.contains("ALARM_TEXT")).map(_._2).toArray.toList

//获取包含user_lable/zh_lable属性的文件
val lable_loc = sc.textFile(LABLE_FILE).map(x => x.split("\\|", -1).zipWithIndex)
val int_lable_loc = lable_loc.map(x => (x.filter(y => y._2 == LABLE_INT_ID_LOC)(0)._1, x.filter(z => z._2 == LABLE_USER_LOC || z._2 ==LABLE_ZH_LOC)))
val map_int_lable = int_lable_loc.map(x => (x._1, x._2.map(_._1))).toArray.toMap

//规则集文件
val rule = sc.textFile(INPUT_FILE).map(x => x.split(",").splitAt(x.split(",").length -1)._1)
val int_rule = rule.flatMap{case x => (0 to x.size - 1).map(y => (x(y),x))}

//原告警数据处理
@transient val conf = new Configuration
conf.set("textinputformat.record.delimiter", LINE_SEP)
val raw_data = sc.newAPIHadoopFile(INPUT_DATA, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf).map(_._2.toString)

val data_filt = raw_data.filter(x => x.split(FIELD_SEP, -1).size >= INVALID_ALA_LEN)
val data_loc = data_filt.map(x => x.split(FIELD_SEP, -1).zipWithIndex.filter{ case (i,j) => filt_loc.toSet(j) })
////
val data_loc_filt = data_loc.filter(x => x.filter(y => y._2 == int_id_loc)(0)._1 != "" && x.filter(y => y._2 == alm_id_loc)(0)._1 != "" )
val data_loc_form = data_loc_filt.map(x => x.map{case (y,`int_id_loc`) => (y.split("\\.", -1)(0), int_id_loc)
                                           	 case (y,`obj_loc`) => (y.split("\\.", -1)(0), obj_loc)
                                                 case (y,`org_type_loc`) => (y.split("\\.", -1)(0),org_type_loc)
                                                 case (y,`res_stat_loc`) => (y.split("\\.", -1)(0),res_stat_loc)
                                            	 case z => z})
val org_sev_filt = data_loc_form.filter(x => x.filter(y => y._2 == org_loc)(0)._1 != "4")
val org_type_filt = org_sev_filt.filter{x => val orty = List("2","3","100","300","301","400","500","501","302")
                                        !orty.toSet(x.filter(y => y._2 == org_type_loc)(0)._1)}
val res_stat_filt = org_type_filt.filter{x => val stat = List("1","1300")
                                         stat.toSet(x.filter(y =>y._2 == res_stat_loc)(0)._1)}
val alm_id_filt = res_stat_filt.filter(x => x.filter(y => y._2 == alm_id_loc)(0)._1.substring(0,2) != "FF")
val stad_flag_filt = alm_id_filt.filter(x => x.filter(y => y._2 == stad_flag_loc)(0)._1 == "2")
	//去掉多余过滤使用的字段，只保留输出要求字段
val data_field = stad_flag_filt.map(x => x.filter{case (x, y) => field_loc.toSet(y)})
////
val data_loc_int_alm = data_field.map(x => ((x.filter(y => y._2 == int_id_loc || y._2 == alm_id_loc).map(_._1).reduce(_ + "_" + _), int_alm_id_loc), x)).map(t => t._1 +: t._2)
val int_alm_data_loc = data_loc_int_alm.map(x => (x.filter(y => y._2 == int_alm_id_loc)(0)._1 ,x))
	//同一个int_alm_id的text字段内容不同，分组后取第一个
val int_data_loc = int_alm_data_loc.groupByKey.map(x => (x._1, x._2.toArray.toList(0)))

//规则集获得原告警信息
val rule_join_data_loc = int_rule.join(int_data_loc).map(_._2).map(x => (x._1.reduce(_ + "," + _), x._2))
val rule_alms_loc = rule_join_data_loc.groupByKey
val alms_loc = rule_alms_loc.map(x => x._2)

//告警正文text字段处理(“”将正文所有字段扩起来，保留正文中的逗号和换行，在转换为csv格式时正常输出)
val text_alms_loc = alms_loc.map(x => x.map(y => ( "\"" ++ y.filter{case (i,j) => text_loc.toSet(j)}.map(_._1).reduce(_ + TEXT_SEP + _) ++ "\"" , y)))
val alms_loc_text_uni = text_alms_loc.map(x => x.map(y =>  y._2.filter{ case (i,j) => !text_loc.toSet(j)} :+ (y._1,alm_text0_loc)))

//加入lable信息
val int_alms = alms_loc_text_uni.map(x => x.map(y => (y.filter(z => z._2 == int_id_loc)(0)._1, y.map(_._1))))
val lable_alms = int_alms.map(x => x.map(y => (map_int_lable.getOrElse(y._1, Array("", "")), y._2 )))
val alms = lable_alms.map(x => x.map(y => (y._2 ++ y._1).reduce(_ + "," + _)))

//输出格式
val index_alm = alms.zipWithIndex.map(x => x._1.mkString(x._2 + "," , "\n"+ x._2 +"," , ""))
//scala.tools.nsc.io.File("disp_8_9.csv").writeAll(alms_csv.toArray.reduce(_ + "\n" +_))
// alms_csv.repartition(1).saveAsTextFile(OUTPUT_FILE)
alms_csv.saveAsTextFile(OUTPUT_FILE)


/////////////////////
//val title_loc = alms_loc_text_uni.first.map(x => x.map(_._2)).toList(0).filter(_ != 0)
//val map_locat = locat.map{case (x,y) => (y,x)}.toArray.toMap
//val int_alm_id_title = "INT_ID_ALARM_ID" +: title_loc.map(x => map_locat(x)) 
//val title_lable = int_alm_id_title ++ Array("USER_LABLE", "ZH_LABLE", "||") 
//val title = title_lable.reduce(_ + "," + _)
//
//val titles_alms = title +: alms_csv.toArray
//scala.tools.nsc.io.File(OUTPUT_FILE).writeAll(titles_alms.reduce(_ + "\n" +_))

