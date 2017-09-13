package cn.itcast.shizhan_storm_mvn.logAnalyze.utils;

import com.google.gson.Gson;

import cn.itcast.shizhan_storm_mvn.logAnalyze.constant.LogTypeConstant;
import cn.itcast.shizhan_storm_mvn.logAnalyze.domain.LogMessage;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月23日 下午2:24:02
 */
public class LogAnalyzeHandler {

	public static LogMessage parser(String line){
		return new Gson().fromJson(line, LogMessage.class);
	}
	
	public static boolean isValidType(int jobType){
		if(jobType==LogTypeConstant.BUY||jobType==LogTypeConstant.CLICK||
				jobType==LogTypeConstant.SEARCH||jobType==LogTypeConstant.VIEW){
			return true;
		}
		return false;
	}
}
