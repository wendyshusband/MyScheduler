package dmir.myscheduler.scheduler.DirectToSlot;

import java.util.HashMap;
import java.util.Map;

public class UniformAssignExecutor implements IAssignExecutor {

    @Override
    public Map calcExecutorAssign(Object[] arg) {
        HashMap<Integer,Integer> numofExecutorToSolt = new HashMap<>();
        int es = (int) arg[0];
        int ss = (int) arg[1];
        int quotient = es / ss;
        int remainder = es % ss;
        for (int i=0; i<ss; i++ ) {
            if (remainder > 0) {
                numofExecutorToSolt.put(i, quotient+1);
                remainder--;
            } else {
                numofExecutorToSolt.put(i, quotient);
            }
        }
        return numofExecutorToSolt;
    }
}
