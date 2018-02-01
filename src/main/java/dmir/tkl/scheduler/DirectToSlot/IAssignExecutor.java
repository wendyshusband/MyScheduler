package dmir.tkl.scheduler.DirectToSlot;

import java.util.Map;

public interface IAssignExecutor<T> {

    Map calcExecutorAssign(T... arg);

}
