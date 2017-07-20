package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;

import java.util.ArrayList;

/**
 * Created by vasgl on 7/20/2017.
 */
public class TaskResponseToComponents {
    private ArrayList<PlatformProxyResourceInfo> platformProxyResourceInfoList;
    private ArrayList<String> resourceIdsForEnablerLogic;
    private Integer count;

    public TaskResponseToComponents() {
        platformProxyResourceInfoList = new ArrayList<>();
        resourceIdsForEnablerLogic = new ArrayList<>();
        count = 0;
    }

    public ArrayList<PlatformProxyResourceInfo> getPlatformProxyResourceInfoList() { return this.platformProxyResourceInfoList; }
    public ArrayList<String> getResourceIdsForEnablerLogic() { return this.resourceIdsForEnablerLogic; }
    public Integer getCount() {return this.count; }

    public void addToResourceIdsForEnablerLogic(String id) {
        resourceIdsForEnablerLogic.add(id);
    }

    public void addToPlatformProxyResourceInfoList(PlatformProxyResourceInfo platformProxyResourceInfo) {
        platformProxyResourceInfoList.add(platformProxyResourceInfo);
    }

    public void addToCount(Integer num) {
        count += num;
    }

    public void add(TaskResponseToComponents taskResponseToComponents) {
        this.platformProxyResourceInfoList.addAll(taskResponseToComponents.getPlatformProxyResourceInfoList());
        this.resourceIdsForEnablerLogic.addAll(taskResponseToComponents.getResourceIdsForEnablerLogic());
        this.count += taskResponseToComponents.getCount();
    }
}