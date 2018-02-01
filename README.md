# This is my scheduler on storm

1. FFD scheduler strategy.
   - We implement the IStrategy interface and define new strategies to schedule our specific topologies. Because, in our experiment, we want the operator assign to slot evenly instead of using the fewest slots.
   - Besides, We can use different configuration to implement the target that assign the specified operator to the specified node.
   
2. Direct to slot scheduler. base on [here](https://github.com/linyiqun/storm-scheduler).
   - This scheduler can implement the target that assign the specified operator to the specified slot.
