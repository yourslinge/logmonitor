package cn.itcast.shizhan_storm_mvn.logAnalyze.domain;
/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月23日 下午9:24:48
 */
public class LogAnalyzeJob {
	private String jobId ;
    private String jobName;
    private int jobType; //1:浏览日志、2:点击日志、3:搜索日志、4:购买日志
    private int bussinessId;
    private int status;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getJobType() {
        return jobType;
    }

    public void setJobType(int jobType) {
        this.jobType = jobType;
    }

    public int getBussinessId() {
        return bussinessId;
    }

    public void setBussinessId(int bussinessId) {
        this.bussinessId = bussinessId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "LogAnalyzeJob{" +
                "jobId='" + jobId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", jobType=" + jobType +
                ", bussinessId=" + bussinessId +
                ", status=" + status +
                '}';
    }
}
