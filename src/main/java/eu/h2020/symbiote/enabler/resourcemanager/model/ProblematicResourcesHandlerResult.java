package eu.h2020.symbiote.enabler.resourcemanager.model;

/**
 * Created by vasgl on 7/20/2017.
 */
public class ProblematicResourcesHandlerResult {
    private ProblematicResourcesHandlerStatus status;
    private TaskInfo taskInfo;

    public ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus status, TaskInfo taskInfo) {
        this.status = status;
        this.taskInfo = taskInfo;
    }

    public ProblematicResourcesHandlerStatus getStatus() { return status; }
    public void setStatus(ProblematicResourcesHandlerStatus status) { this.status = status; }

    public TaskInfo getTaskInfo() { return taskInfo; }
    public void setTaskInfo(TaskInfo taskInfo) { this.taskInfo = taskInfo; }
}
