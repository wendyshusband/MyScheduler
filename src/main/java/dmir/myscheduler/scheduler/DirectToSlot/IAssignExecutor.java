package dmir.myscheduler.scheduler.DirectToSlot;

import java.util.Map;

public interface IAssignExecutor<T> {

    Map calcExecutorAssign(T... arg);

}
