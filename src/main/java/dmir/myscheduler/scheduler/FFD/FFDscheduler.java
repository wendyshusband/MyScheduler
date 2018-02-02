package dmir.myscheduler.scheduler.FFD;

import clojure.lang.PersistentArrayMap;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by 44931 on 2017/12/30.
 */
public class FFDscheduler implements IScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(FFDscheduler.class);

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("DirectScheduler begin schedule!");

        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;
        //TopologyDetails topology;

        //scheduling flag, which user set in conf file.
        String assignedFlag;
        Map conf;
//        Iterator<String> iterator = null;

        topologyDetailes = topologies.getTopologies();
        for (TopologyDetails td: topologyDetailes) {
            conf = td.getConf();
            assignedFlag = (String)conf.get("assigned_flag");
            if(assignedFlag != null && assignedFlag.equals("1")){
                LOG.info("finding topology named " + td.getName());
                topologyAssign(cluster, td, conf);
            }
        }

        //other topology scheduling by RAS.
        new ResourceAwareScheduler().schedule(topologies, cluster);
    }

    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map map) {
        Set<String> keys;
        PersistentArrayMap designMap;
        Iterator<String> iterator;

        iterator = null;
        // make sure the special topology is submitted,
        if (topology != null) {
            designMap = (PersistentArrayMap)map.get("design_map");
            if(designMap != null){
                System.out.println("design map size is " + designMap.size());
                keys = designMap.keySet();
                iterator = keys.iterator();

                System.out.println("keys size is " + keys.size());
            }

            if(designMap == null || designMap.size() == 0){
                System.out.println("design map is null");
            }

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                System.out.println("Our special topology does not need scheduling.");
            } else {
                System.out.println("Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }

                String componentName;
                String nodeName;
                if(designMap != null && iterator != null){
                    while (iterator.hasNext()){
                        componentName = iterator.next();
                        nodeName = (String)designMap.get(componentName);

                        System.out.println("现在进行调度 组件名称->节点名称:" + componentName + "->" + nodeName);
                        componentAssign(cluster, topology, componentToExecutors, componentName, nodeName);
                    }
                }
            }
        }
    }

    /**
     * 组件调度
     * @param cluster
     * 集群的信息
     * @param topology
     * 待调度的拓扑细节信息
     * @param totalExecutors
     * 组件的执行器
     * @param componentName
     * 组件的名称
     * @param supervisorName
     * 节点的名称
     */
    private void componentAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> totalExecutors, String componentName, String supervisorName){
        if (!totalExecutors.containsKey(componentName)) {
            System.out.println("Our special-spout does not need scheduling.");
        } else {
            System.out.println("Our special-spout needs scheduling.");
            List<ExecutorDetails> executors = totalExecutors.get(componentName);

            // find out the our "special-supervisor" from the supervisor metadata
            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
            //cluster.getAvailablePorts()
            SupervisorDetails specialSupervisor = null;
            for (SupervisorDetails supervisor : supervisors) {
                Map meta = (Map) supervisor.getSchedulerMeta();

                if(meta != null && meta.get("name") != null){
                    System.out.println("supervisor name:" + meta.get("name"));

                    if (meta.get("name").equals(supervisorName)) {
                        System.out.println("Supervisor finding");
                        specialSupervisor = supervisor;
                        break;
                    }
                }else {
                    System.out.println("Supervisor meta null");
                }

            }

            // found the special supervisor
            if (specialSupervisor != null) {
                System.out.println("Found the special-supervisor");
                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                // 如果目标节点上已经没有空闲的slot,则进行强制释放
                if (availableSlots.isEmpty() && !executors.isEmpty()) {
                    for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                        cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                    }
                }

                // 重新获取可用的slot
                availableSlots = cluster.getAvailableSlots(specialSupervisor);

                // 选取节点上第一个slot,进行分配
                cluster.assign(availableSlots.get(0), topology.getId(), executors);
                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
            } else {
                System.out.println("There is no supervisor find!!!");
            }
        }
    }
//    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map conf) {
////        Set<String> keys;
////        PersistentArrayMap designMap;
//        Iterator<String> iterator;
//
//        iterator = null;
//        // make sure the special topology is submitted,
//        if (topology != null) {
//            //designMap = (PersistentArrayMap)conf.get("design_map");
////            if(designMap != null){
////                LOG.info("design map size is " + designMap.size());
////                keys = designMap.keySet();
////                iterator = keys.iterator();
////
////                LOG.info("keys size is " + keys.size());
////            }
////
////            if(designMap == null || designMap.size() == 0){
////                LOG.info("design map is null");
////            }
//
//            boolean needsScheduling = cluster.needsScheduling(topology);
//
//            if (!needsScheduling) {
//                LOG.info("Our special topology does not need scheduling.");
//            } else {
//                LOG.info("Our special topology needs scheduling.");
//                // find out all the needs-scheduling components of this topology
//                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
//
//                LOG.info("needs scheduling(component->executor): " + componentToExecutors);
//                LOG.info("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
//                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
//                if (currentAssignment != null) {
//                    LOG.info("current assignments: " + currentAssignment.getExecutorToSlot());
//                } else {
//                    LOG.info("current assignments: {}");
//                }
//
//                String componentName;
//                String nodeName;
//                // designMap != null &&
//                if(iterator != null){
//                    while (iterator.hasNext()){
//                        componentName = iterator.next();
//                        //nodeName = (String)designMap.get(componentName);
//
////                        LOG.info("现在进行调度 组件名称->节点名称:" + componentName + "->" + nodeName);
//                       // componentAssign(cluster, topology, componentToExecutors, componentName);
//                    }
//                }
//            }
//        }
//    }
}
