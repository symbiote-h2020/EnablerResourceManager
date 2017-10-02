package eu.h2020.symbiote.enabler.resourcemanager.repository;

import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

/**
 * Created by vasgl on 7/18/2017.
 */
@RepositoryRestResource(collectionResourceRel = "taskInfo", path = "taskInfo")
public interface TaskInfoRepository extends MongoRepository<TaskInfo, String> {
    public TaskInfo findByTaskId(String taskId);
}
