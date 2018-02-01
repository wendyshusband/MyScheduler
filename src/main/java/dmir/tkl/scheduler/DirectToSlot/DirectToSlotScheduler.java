package dmir.tkl.scheduler.DirectToSlot;

import clojure.lang.PersistentArrayMap;
import javafx.util.Pair;
import org.apache.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DirectToSlotScheduler implements IScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DirectToSlotScheduler.class);

    @Override
    public void prepare(Map map) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("Direct assign to slot, The Scheduler begin schedule!");

        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;

        //scheduling flag, which user set in conf file.
        String assignedFlag;
        Map conf;

        topologyDetailes = topologies.getTopologies();
        for (TopologyDetails td: topologyDetailes) {
            conf = td.getConf();
            assignedFlag = (String)conf.get("assigned_flag");
            if(assignedFlag != null && assignedFlag.equals("1")){
                LOG.info("finding topology named " + td.getName());
                topologyAssign(cluster, td, conf);
            }
        }

        //other topology scheduling by default scheduler.
        new EvenScheduler().schedule(topologies, cluster);
    }

    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map conf) {
        Set<String> keys;
        PersistentArrayMap designMap;
        Iterator<String> iterator;
        iterator = null;

        // make sure the special topology is submitted
        if (topology != null) {
            designMap = (PersistentArrayMap)conf.get("design_map");
            if(designMap != null){
                LOG.debug("design map size is " + designMap.size());
                keys = designMap.keySet();
                iterator = keys.iterator();
                LOG.debug("keys size is " + keys.size());
            }
            if(designMap == null || designMap.size() == 0){
                LOG.error("design map is null");
                throw new NullPointerException();
            }

            boolean needsScheduling = cluster.needsScheduling(topology);
            if (!needsScheduling) {
                LOG.warn("Your special topology does not need scheduling.");
            } else {
                LOG.info("Your special topology {} needs scheduling.", topology.getName());
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                LOG.debug("needs scheduling(component->executor): " + componentToExecutors);
                LOG.debug("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    LOG.info("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    LOG.info("current assignments: {}");
                }

                String componentName;
                String assignListStr;
                ArrayList<Pair> slotList;
                while (iterator.hasNext()) {
                    componentName = iterator.next();
                    assignListStr = (String) designMap.get(componentName);
                    LOG.debug("assignListStr "+assignListStr);
                    slotList = parseSlotList(assignListStr);
                    slotAssign(cluster, topology, componentToExecutors, componentName, slotList);
                }
                //get need scheduling component again. (system bolt)
                componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                slotAssignForSystemBolt(cluster, topology, componentToExecutors);
            }
        }
    }

    private void slotAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> totalExecutors, String componentName, ArrayList<Pair> slotList) {

        LOG.info(componentName);
        LOG.info(" exe:"+totalExecutors);
        List<ExecutorDetails> executors = totalExecutors.get(componentName);
        double check = executors.size() * 1.0 / slotList.size();

        if (check < 1.0) {
            LOG.error("You need assign more slot for "+componentName);
            throw new IllegalArgumentException();
        } else {
            Integer[] param = new Integer[2];
            param[0] = executors.size();
            param[1] = slotList.size();
            HashMap<Integer,Integer> numofExecutorToSolt = (HashMap) new UniformAssignExecutor().calcExecutorAssign(param);

            LOG.debug("numofExecutorToSolt:"+numofExecutorToSolt);
            int cursor = 0;
            String nodeName;
            Integer port;
            List<SupervisorDetails> supervisorList;

            for (int i = 0; i < slotList.size(); i++) {
                nodeName = (String) slotList.get(i).getKey();
                port = (Integer) slotList.get(i).getValue();
                supervisorList = cluster.getSupervisorsByHost(nodeName);
                if (supervisorList != null) {
                    for (SupervisorDetails supervisor : supervisorList) {
                        Set<Integer> availablePorts = cluster.getAvailablePorts(supervisor);
                        if (!availablePorts.isEmpty() && availablePorts.contains(port)) {
                            List<ExecutorDetails> assignExecutor = new LinkedList<>();
                            int length = numofExecutorToSolt.get(i);
                            int j = cursor;
                            for (; j < cursor + length && j < executors.size(); j++) {
                                assignExecutor.add(executors.get(j));
                            }
                            cursor = j;
                            //Do assignment.
                            LOG.info(supervisor.getHost() + "::" + port + "->" + componentName);
                            assignExecutor.forEach(e -> LOG.info("{" + e.getStartTask() + "~" + e.getEndTask() + "}"));
                            cluster.assign(new WorkerSlot(supervisor.getId(), port), topology.getId(), assignExecutor);
                            break;
                        }
                        // if the workslot not enough or used. there will assign randomly.
                    }
                }
            }
            LOG.info("assignment for "+componentName+" success!");
        }
    }

    private void slotAssignForSystemBolt(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> totalExecutors) {
        LOG.debug("exeutors:"+totalExecutors);
        for (Map.Entry entry : totalExecutors.entrySet()) {
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            cluster.assign(availableSlots.get(0), topology.getId(), (Collection<ExecutorDetails>) entry.getValue());
            LOG.info("assignment for "+entry.getKey()+" success!");
        }
    }

    private ArrayList parseSlotList(String assignList) {
        ArrayList<Pair> slotList = new ArrayList<>();
        Pair<String,Integer> slot;
        String[] pairs = assignList.split(";");
        for (String pair : pairs) {
            String[] temp = pair.split(":");
            slot = new Pair<>(temp[0],Integer.valueOf(temp[1]));
            slotList.add(slot);
        }
        return slotList;
    }
}
