#!/usr/bin/env python3
"""
================================================================================
测试套件：test_scheduler.py
自动化验证 Mini Coroutine Scheduler 的状态机流转、睡眠定时堆、异步队列与并发正确性。
================================================================================
"""

import time
from scheduler import Scheduler, TaskState, AsyncQueue, AsyncEvent, sleep, yield_cpu


def test_1_task_lifecycle():
    print("\n[测试 1] 任务状态机流转 (READY -> RUNNING -> TERMINATED) 验证...")
    sched = Scheduler()
    logs = []

    def task_coro():
        logs.append("step 1")
        yield from yield_cpu()
        logs.append("step 2")
        return "SUCCESS"

    task = sched.spawn(task_coro, name="DemoTask")
    assert task.state == TaskState.READY

    sched.run_until_idle()

    assert task.state == TaskState.TERMINATED
    assert task.result == "SUCCESS"
    assert logs == ["step 1", "step 2"]
    print("  ✅ [PASS] 状态机与时间片上下文让出验证通过！")


def test_2_timer_and_sleep_heap():
    print("\n[测试 2] 最小堆定时器与非阻塞 Sleep 调度精度验证...")
    sched = Scheduler()
    timeline = []

    def fast_task():
        yield from sleep(0.05)
        timeline.append("fast_done")

    def slow_task():
        yield from sleep(0.12)
        timeline.append("slow_done")

    sched.spawn(slow_task, name="Slow")
    sched.spawn(fast_task, name="Fast")

    t0 = time.time()
    sched.run_until_idle()
    elapsed = time.time() - t0

    assert timeline == ["fast_done", "slow_done"], f"定时执行顺序错误: {timeline}"
    assert 0.11 <= elapsed <= 0.20, f"调度总时间偏离预期: {elapsed:.3f}s"
    print(f"  -> 调度耗时: {elapsed:.3f}s，定时顺序: {timeline}")
    print("  ✅ [PASS] 睡眠小顶堆定时唤醒验证通过！")


def test_3_async_queue_producer_consumer():
    print("\n[测试 3] 异步阻塞队列 (AsyncQueue) 生产与消费同步验证...")
    sched = Scheduler()
    queue = AsyncQueue(sched, maxsize=1)
    consumed_items = []

    def producer():
        for i in range(3):
            yield from queue.put(f"item_{i}")

    def consumer():
        for _ in range(3):
            val = yield from queue.get()
            consumed_items.append(val)

    sched.spawn(producer, name="Prod")
    sched.spawn(consumer, name="Cons")
    sched.run_until_idle()

    assert consumed_items == ["item_0", "item_1", "item_2"]
    assert queue.qsize() == 0
    print("  ✅ [PASS] 生产者-消费者与条件挂起/唤醒机制验证通过！")


def test_4_async_event_synchronization():
    print("\n[测试 4] 异步事件 (AsyncEvent) 条件变量与多任务广播唤醒验证...")
    sched = Scheduler()
    event = AsyncEvent(sched)
    awake_tasks = []

    def waiter(name):
        yield from event.wait()
        awake_tasks.append(name)

    def trigger():
        yield from sleep(0.05)
        event.set()

    sched.spawn(waiter("W1"))
    sched.spawn(waiter("W2"))
    sched.spawn(waiter("W3"))
    sched.spawn(trigger)

    sched.run_until_idle()

    assert len(awake_tasks) == 3, "未唤醒全部等待任务"
    print(f"  -> 广播唤醒任务列表: {awake_tasks}")
    print("  ✅ [PASS] 异步事件广播唤醒验证通过！")


def main():
    print("=" * 60)
    print("🧪 开始执行 Mini Coroutine Scheduler 自动化测试套件")
    print("=" * 60)

    test_1_task_lifecycle()
    test_2_timer_and_sleep_heap()
    test_3_async_queue_producer_consumer()
    test_4_async_event_synchronization()

    print("\n" + "=" * 60)
    print("🎉 全部 4 项操作系统调度与同步测试均已顺利通过！")
    print("=" * 60)


if __name__ == "__main__":
    main()
