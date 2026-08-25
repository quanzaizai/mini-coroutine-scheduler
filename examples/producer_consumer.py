#!/usr/bin/env python3
"""
================================================================================
经典操作系统问题演示：生产者-消费者模型 (Producer-Consumer Problem)
利用自制微型协程调度器与 AsyncQueue 实现无锁高效协同。
================================================================================
"""

import time
from scheduler import Scheduler, AsyncQueue, sleep


def producer(name: str, queue: AsyncQueue, count: int):
    print(f"🏭 [{name}] 生产者启动，准备生产 {count} 个产品...")
    for i in range(1, count + 1):
        yield from sleep(0.05)  # 模拟生产耗时
        item = f"产品-{i} (来自 {name})"
        print(f"📦 [{name}] 生产了: {item}，尝试放入队列 (当前队列深度: {queue.qsize()})")
        yield from queue.put(item)
    print(f"✅ [{name}] 全部生产完毕！")


def consumer(name: str, queue: AsyncQueue, count: int):
    print(f"🛒 [{name}] 消费者启动，准备消费 {count} 个产品...")
    for _ in range(count):
        item = yield from queue.get()
        print(f"🍽️ [{name}] 消费了: {item}")
        yield from sleep(0.08)  # 模拟消费耗时
    print(f"🎉 [{name}] 全部消费完毕！")


def main():
    print("=" * 65)
    print("🎬 启动经典操作系统『生产者-消费者模型』协程实战")
    print("=" * 65)

    sched = Scheduler()
    # 创建一个缓冲区上限为 2 的阻塞队列
    bounded_queue = AsyncQueue(sched, maxsize=2)

    # 启动 2 个生产者与 1 个消费者
    sched.spawn(producer("生产者-A", bounded_queue, 3), name="ProducerA")
    sched.spawn(producer("生产者-B", bounded_queue, 3), name="ProducerB")
    sched.spawn(consumer("消费者-1", bounded_queue, 6), name="Consumer1")

    start_time = time.time()
    sched.run_until_idle()
    elapsed = time.time() - start_time

    print("=" * 65)
    print(f"✨ 调度完成！总耗时: {elapsed:.2f}s | 调度器事件循环周期: {sched.cycle_count} 次")
    print("=" * 65)


if __name__ == "__main__":
    main()
