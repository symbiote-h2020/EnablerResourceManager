package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.TimerTask;

/**
 * Created by vasgl on 7/2/2017.
 */
public class ScheduledTaskInfoUpdate extends TimerTask {

    private static Log log = LogFactory.getLog(ScheduledTaskInfoUpdate.class);

    private TaskInfoRepository taskInfoRepository;
    private SearchHelper searchHelper;
    private TaskInfo taskInfo;

    public ScheduledTaskInfoUpdate(TaskInfoRepository taskInfoRepository, SearchHelper searchHelper,
                                   TaskInfo taskInfo) {

        Assert.notNull(taskInfoRepository,"TaskInfoRepository can not be null!");
        this.taskInfoRepository = taskInfoRepository;

        Assert.notNull(searchHelper,"SearchHelper can not be null!");
        this.searchHelper = searchHelper;

        Assert.notNull(taskInfo,"TaskInfo can not be null!");
        this.taskInfo = taskInfo;
    }

    public void run() {

        log.debug("Updating taskInfo with id: " + taskInfo.getTaskId());
        QueryAndProcessSearchResponseResult result = searchHelper.queryAndProcessSearchResponse(taskInfo);

        taskInfo.setStoredResourceIds(new ArrayList<>(result.getTaskInfo().getStoredResourceIds()));
        taskInfoRepository.save(taskInfo);
    }

}
