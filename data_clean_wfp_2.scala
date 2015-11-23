import scala.io
import scala.math.pow
import java.util.Date
import java.util.Calendar
import scala.collection.mutable.MutableList
import java.util.concurrent.TimeUnit

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
//设置行分割符，读入原始文件
val INPUT_FILE = "/user/boco/dataplatform/shanxi/input/2014_0*"
val CONF_FILE = "/user/boco/datamine/confFile/conf_data_clean.txt"
val OBJECT_CLASS_FILE = "/user/boco/datamine/redisDatamine/AWJ_NETSOURCE.csv"
val OUTPUT_FILE = "datamine/outputFile/CLEANED_8_9"
     
val LINE = "\\ |\n"
val SEP = "_\\|"
val INVALID_ALA_LEN = 163
val SEP_OUTPUT = "\001" 
val START_TIME = "2014-08-01 00:00:00"
val END_TIME = "2014-10-00 00:00:00"
val STEP = 5
val SLIP = 3

@transient val conf = new Configuration
conf.set("textinputformat.record.delimiter", LINE)
//读入字段截取文件，过滤掉不完整的后，只留下字段截取文件中的字段
val raw_data = sc.newAPIHadoopFile(INPUT_FILE, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf).map(_._2.toString)
val data = raw_data.filter(x => x.split(SEP).size >= INVALID_ALA_LEN)
val conf = sc.textFile(CONF_FILE).map(x => (x.split("=")(0), x.split("=")(1).toInt))
val conf_loc = conf.map(_._2).toArray.toList
val obj = sc.textFile(OBJECT_CLASS_FILE)

val data_loc = data.map(x => x.split(SEP).zipWithIndex.filter{ case (i,j) => conf_loc.toSet(j) && i != "" })
val conf_num = conf.count
//字段截取文件中的字段对应的数字位置
val eve_time_loc = conf.filter(x => x._1 == "EVENT_TIME").map(_._2).toArray.toList(0)
val int_id_loc = conf.filter(x => x._1 == "INT_ID").map(_._2).toArray.toList(0) 
val alm_id_loc = conf.filter(x => x._1 == "ALARM_ID").map(_._2).toArray.toList(0) 
val obj_loc= conf.filter(x => x._1 == "OBJECT_CLASS").map(_._2).toArray.toList(0) 
val org_loc= conf.filter(x => x._1 == "ORG_SEVERITY").map(_._2).toArray.toList(0) 
val org_type_loc = conf.filter(x => x._1 == "ORG_TYPE").map(_._2).toArray.toList(0)
val res_stat_loc = conf.filter(x => x._1 == "RESOURCE_STATUS").map(_._2).toArray.toList(0)
val stad_flag_loc = conf.filter(x => x._1 == "STANDARD_FLAG").map(_._2).toArray.toList(0)
//设置时间格式
val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
val start_date = date_format.parse(START_TIME)
val end_date = date_format.parse(END_TIME)
val start_cal = Calendar.getInstance()
val end_cal = Calendar.getInstance() 
start_cal.setTime(start_date)
end_cal.setTime(end_date)
//若eve_time在给定的起始、结束时间里返回true，反之false
def filtEveTime(eve_time:String): Boolean = {
    val eve_cal = Calendar.getInstance()
    eve_cal.setTime(date_format.parse(eve_time))
    if(eve_cal.after(start_cal) && eve_cal.before(end_cal)) true else false
}
//数据过滤掉乱码的部分
val data_loc_form = data_loc.map(x => x.map{case (y,`int_id_loc`) => (y.split("\\.", -1)(0), int_id_loc)
                                            case (y,`obj_loc`) => (y.split("\\.", -1)(0),obj_loc) 
					    case (y,`org_type_loc`) => (y.split("\\.", -1)(0),org_type_loc)
					    case (y,`res_stat_loc`) => (y.split("\\.", -1)(0),res_stat_loc)
					    case z => z})
//数据过滤掉不符合业务的部分
val int_time_filt = data_loc_form.filter(x => x.size == conf_num && (x.filter(y => y._2 == int_id_loc).toList(0)._1 != "0") && filtEveTime(x.filter(y => y._2 ==  eve_time_loc).toList(0)._1) )
val org_sev_filt = int_time_filt.filter(x => x.filter(y => y._2 == org_loc)(0)._1 != "4")
val org_type_filt = org_sev_filt.filter{x => val orty = List("2","3","100","300","301","400","500","501","302")
                                        !orty.toSet(x.filter(y => y._2 == org_type_loc)(0)._1)}
val res_stat_filt = org_type_filt.filter{x => val stat = List("1","1300")
                                         stat.toSet(x.filter(y =>y._2 == res_stat_loc)(0)._1)}
val alm_id_filt = res_stat_filt.filter(x => x.filter(y => y._2 == alm_id_loc)(0)._1.substring(0,2) != "FF")
val stad_flag_filt = alm_id_filt.filter(x => x.filter(y => y._2 == stad_flag_loc)(0)._1 == "2")

val int_alm_data_loc = stad_flag_filt.map(x => ((x.filter(y => y._2 == int_id_loc || y._2 == alm_id_loc).map(_._1).reduce(_ + "_" + _), int_id_loc), x.filter(z => z._2 != int_id_loc && z._2 != alm_id_loc))).map(t => t._1 +: t._2)
val obj_map = obj.map(x => (x.split(",")(0), x.split(",")(1))).toArray.toMap
val obj_class_data_loc = int_alm_data_loc.map(x => (x.filter(y => y._2 == obj_loc).map(z => obj_map.getOrElse(z._1,"3")), x.filter(w => w._2 != obj_loc)))
val wei_data_loc = obj_class_data_loc.map(x => x._2.filter(y => y._2 != org_loc) ++ x._2.filter(y => y._2 == org_loc).map(z => ((pow(3, 4 - z._1.toInt) + pow(3, 3 -x._1(0).toInt)).toFloat/156, obj_loc)))
val int_time_wei_loc = wei_data_loc.map(x => x.filter(y => y._2 == int_id_loc || y._2 == eve_time_loc || y._2 == obj_loc))
  
def convertMinu(minu:Int) = {
    val conv_cal = Calendar.getInstance()
    conv_cal.setTime(start_cal.getTime)
    conv_cal.add(java.util.Calendar.MINUTE, minu)
    date_format.format(conv_cal.getTime)
}

def buildWin(eve_time:String) : Array[(String, String)] = {
    val eve_date = date_format.parse(eve_time)
    val eve_cal = Calendar.getInstance()
    eve_cal.setTime(eve_date)
  
    val time_point = eve_date.getTime - start_date.getTime
    val time_point_minu = TimeUnit.MINUTES.convert(time_point, TimeUnit.MILLISECONDS) 
    //start_point = 0
    val end_point = end_date.getTime - start_date.getTime
    val end_point_minu = TimeUnit.MINUTES.convert(end_point, TimeUnit.MILLISECONDS)	

    if(time_point_minu - STEP <= 0){
        val windows_point = Array((0.toInt, STEP))
        val wins_format = windows_point.map(x => (convertMinu(x._1), convertMinu(x._2)))
        return wins_format
    }
    val first_start = ((time_point_minu - STEP) / SLIP + 1) * SLIP
    val last_start = (time_point_minu / SLIP) * SLIP
    val windows_point = Range(first_start.toInt, last_start.toInt + 1, SLIP).toArray.map(x => (x, x + STEP)).filter(x => x._2 <= end_point_minu)
    val wins_format = windows_point.map(x => (convertMinu(x._1) , convertMinu(x._2)))
    return wins_format
}

val time_alm = int_time_wei_loc.map(x => (x.filter(y => y._2 == eve_time_loc).map(_._1).toList(0) , x.filter(y => y._2 != eve_time_loc).map(_._1)))
val win_alm = time_alm.flatMap(x => buildWin(x._1.toString).map(y => y._1 + "," + y._2).map(_ +: x._2)) 
val alms = win_alm.map(x => x.reduce(_ + SEP_OUTPUT + _)).distinct 
alms.saveAsTextFile(OUTPUT_FILE)
