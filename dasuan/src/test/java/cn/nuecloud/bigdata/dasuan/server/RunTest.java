package cn.nuecloud.bigdata.dasuan.server;

import cn.neucloud.bigdata.taskserver.bean.Task;
import cn.neucloud.bigdata.taskserver.service.TaskLogService;
import cn.neucloud.bigdata.taskserver.service.TaskService;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * 测试dubbo
 * Created by eeve on 16/10/19.
 */
@Slf4j
public class RunTest {

    @org.junit.Test
    public void testMain() throws Exception {
        Run run = Run.getInstance();
        TaskLogService taskLogService = run.getBean(TaskLogService.class);
        TaskService taskService = run.getBean(TaskService.class);
        int id = taskService.addGetId(
                Task.builder()
                        .algorithmId(1)
                        .createTime(new Date())
                        .description("备注信息。。。")
                        .build()
        );
        if(id != 0) { // 为0时表示失败
            log.info("!!! --------- add task success, id: {}", id);
        } else {
            log.error("增加task失败！");
        }
    }
}