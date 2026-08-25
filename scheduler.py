#!/usr/bin/env python3
"""
================================================================================
项目名称：Mini Coroutine Scheduler（微型用户态协程调度器）
所属学科：《操作系统》—— 进程/线程管理、上下文切换与调度算法
核心实现原理：
  1. 【操作系统核心知识点】：进程/线程生命周期状态机 (READY, RUNNING, SLEEPING, WAITING, TERMINATED)
  2. 【用户态上下文切换】：利用 Python 生成器机制模拟底层 CPU 寄存器与调用栈帧的保存与恢复 (Yield & Resume)
  3. 【调度算法与事件循环】：轮转调度就绪队列 + 最小堆定时器队列（Time Wheel / Sleep Queue）
  4. 【进程间通信与同步机制 (IPC)】：实现异步锁 (AsyncLock)、事件 (AsyncEvent) 与通道 (AsyncQueue)
================================================================================
"""

import time
import heapq
from enum import Enum, auto
from collections import deque
from typing import Any, Callable, Generator, List, Optional, Tuple


# ==============================================================================
# 【操作系统知识点 1】：任务/进程生命周期状态 (Task State Machine)
# ==============================================================================
class TaskState(Enum):
    READY = auto()        # 就绪态：已分配必要资源，进入就绪队列，等待 CPU 调度
    RUNNING = auto()      # 运行态：正在占用 CPU 执行
    SLEEPING = auto()     # 睡眠态：调用了 sleep，进入时间等待堆
    WAITING = auto()      # 阻塞态：等待事件、锁或队列数据 (I/O Wait / Lock Wait)
    TERMINATED = auto()   # 终止态：任务已正常结束或异常退出，等待回收资源


# ==============================================================================
# 【操作系统知识点 2】：任务控制块 (TCB - Task Control Block)
# 对应操作系统内核中描述进程/线程属性与上下文的数据结构 (如 Linux 的 task_struct)
# ==============================================================================
class Task:
    _id_counter = 1

    def __init__(self, coro: Generator, name: Optional[str] = None):
        self.tid = Task._id_counter
        Task._id_counter += 1
        self.name = name or f"Task-{self.tid}"
        self.coro = coro                            # 协程对象（保存执行上下文、局部变量与 PC 指针）
        self.state = TaskState.READY                # 初始状态为就绪态
        self.wake_time: Optional[float] = None      # 睡眠唤醒绝对时间戳
        self.result: Any = None                     # 任务返回值
        self.exception: Optional[Exception] = None  # 异常记录
        self.steps_executed = 0                     # 执行的时间片步数计数

    def resume(self, send_val: Any = None) -> Any:
        """
        【上下文恢复 (Context Switch In)】：
        恢复该任务的执行上下文，直到它主动调用 yield 交出 CPU 控制权。
        """
        self.state = TaskState.RUNNING
        self.steps_executed += 1
        return self.coro.send(send_val)

    def __repr__(self):
        return f"<Task {self.name}(TID={self.tid}) State={self.state.name}>"


# ==============================================================================
# 【操作系统知识点 3】：微型用户态调度器与事件循环 (Scheduler & Event Loop)
# ==============================================================================
class Scheduler:
    def __init__(self):
        # 就绪队列 (Ready Queue)：FIFO 先入先出调度
        self.ready_queue: deque[Task] = deque()

        # 睡眠等待小顶堆 (Sleeping Min-Heap): 按 wake_time 升序排列 (wake_time, tid, task)
        self.sleeping_heap: List[Tuple[float, int, Task]] = []

        # 阻塞等待池 (Waiting Set)
        self.waiting_tasks: set[Task] = set()

        # 运行中的任务
        self.current_task: Optional[Task] = None
        self.is_running = False
        self.cycle_count = 0

    def spawn(self, coro_or_func: Any, name: Optional[str] = None) -> Task:
        """
        创建新任务（相当于操作系统的 fork / pthread_create）。
        将任务加入就绪队列。
        """
        if not isinstance(coro_or_func, Generator):
            # 如果传入的是普通生成器函数，先调用生成生成器对象
            coro = coro_or_func()
        else:
            coro = coro_or_func

        task = Task(coro, name=name)
        self.ready_queue.append(task)
        return task

    # --------------------------------------------------------------------------
    # 核心事件循环 (The Event Loop)
    # --------------------------------------------------------------------------
    def run_until_idle(self):
        """
        【操作系统调度核心主循环】：
        1. 检查睡眠堆，将到期的任务从睡眠队列移入就绪队列 (Wake up)
        2. 从就绪队列头弹出一个任务 (Pick next task to run)
        3. 执行上下文切换，运行一个时间片 (Context switch & Execute)
        4. 根据任务 yield 出来的系统调用指令 (Syscall)，改变任务状态
        """
        self.is_running = True

        while self.is_running:
            now = time.time()

            # 1. 唤醒所有到期的睡眠任务
            while self.sleeping_heap and self.sleeping_heap[0][0] <= now:
                _, _, task = heapq.heappop(self.sleeping_heap)
                if task.state == TaskState.SLEEPING:
                    task.state = TaskState.READY
                    self.ready_queue.append(task)

            # 2. 如果就绪队列为空，且没有其他待唤醒任务，则调度结束
            if not self.ready_queue:
                if not self.sleeping_heap and not self.waiting_tasks:
                    break
                # 若就绪队列为空但有睡眠任务，休眠至最近的唤醒时刻（避免忙等待 CPU 100%）
                if self.sleeping_heap:
                    sleep_duration = max(0.001, self.sleeping_heap[0][0] - time.time())
                    time.sleep(min(sleep_duration, 0.05))
                continue

            # 3. 从就绪队列提取下一个任务
            task = self.ready_queue.popleft()
            self.current_task = task
            self.cycle_count += 1

            # 4. 执行一个时间片
            try:
                syscall = task.resume()

                # 处理任务发出的模拟内核系统调用 (Syscall)
                if isinstance(syscall, SyscallSleep):
                    task.state = TaskState.SLEEPING
                    wake_time = time.time() + syscall.duration
                    task.wake_time = wake_time
                    heapq.heappush(self.sleeping_heap, (wake_time, task.tid, task))

                elif isinstance(syscall, SyscallYield):
                    task.state = TaskState.READY
                    self.ready_queue.append(task)

                elif isinstance(syscall, SyscallWait):
                    task.state = TaskState.WAITING
                    self.waiting_tasks.add(task)

                else:
                    # 默认隐式交出时间片
                    task.state = TaskState.READY
                    self.ready_queue.append(task)

            except StopIteration as e:
                # 任务正常执行结束
                task.state = TaskState.TERMINATED
                task.result = e.value
            except Exception as e:
                # 任务发生未捕获异常
                task.state = TaskState.TERMINATED
                task.exception = e
                print(f"[调度器警告] 任务 {task.name} 异常终止: {e}")
            finally:
                self.current_task = None

        self.is_running = False

    def wake_task(self, task: Task):
        """将某个阻塞等待的任务唤醒并重新移入就绪队列"""
        if task in self.waiting_tasks:
            self.waiting_tasks.remove(task)
            task.state = TaskState.READY
            self.ready_queue.append(task)


# ==============================================================================
# 【操作系统知识点 4】：模拟系统调用 (Simulated System Calls)
# ==============================================================================
class SyscallSleep:
    """非阻塞定时休眠系统调用"""
    def __init__(self, duration: float):
        self.duration = duration


class SyscallYield:
    """主动让出 CPU 时间片系统调用"""
    pass


class SyscallWait:
    """阻塞等待事件/信号系统调用"""
    pass


# 对外暴露的便捷协程 API
def sleep(seconds: float):
    return (yield SyscallSleep(seconds))


def yield_cpu():
    return (yield SyscallYield())


# ==============================================================================
# 【操作系统知识点 5】：进程间通信与同步原语 (IPC: AsyncQueue & AsyncEvent)
# ==============================================================================
class AsyncQueue:
    """
    异步非阻塞先进先出通道 (Bounded/Unbounded Async Queue)
    解决经典的“生产者-消费者问题”，支持队列满时阻塞生产者、队列空时阻塞消费者。
    """
    def __init__(self, scheduler: Scheduler, maxsize: int = 0):
        self.scheduler = scheduler
        self.maxsize = maxsize
        self._queue = deque()
        self._get_waiters: deque[Task] = deque()
        self._put_waiters: deque[Task] = deque()

    def put(self, item: Any):
        """生产者放入元素；若队列已满则挂起等待"""
        while self.maxsize > 0 and len(self._queue) >= self.maxsize:
            current = self.scheduler.current_task
            self._put_waiters.append(current)
            yield SyscallWait()

        self._queue.append(item)
        # 唤醒等待数据的消费者
        if self._get_waiters:
            w = self._get_waiters.popleft()
            self.scheduler.wake_task(w)

    def get(self) -> Any:
        """消费者取出元素；若队列为空则挂起等待"""
        while not self._queue:
            current = self.scheduler.current_task
            self._get_waiters.append(current)
            yield SyscallWait()

        item = self._queue.popleft()
        # 唤醒等待空间的生产者
        if self._put_waiters:
            w = self._put_waiters.popleft()
            self.scheduler.wake_task(w)

        return item

    def qsize(self) -> int:
        return len(self._queue)


class AsyncEvent:
    """异步事件同步原语（类似操作系统的条件变量/信号量）"""
    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self._is_set = False
        self._waiters: List[Task] = []

    def set(self):
        """设置事件为真，唤醒所有阻塞等待的任务"""
        self._is_set = True
        while self._waiters:
            t = self._waiters.pop()
            self.scheduler.wake_task(t)

    def clear(self):
        self._is_set = False

    def is_set(self) -> bool:
        return self._is_set

    def wait(self):
        """若事件未触发则阻塞当前任务"""
        while not self._is_set:
            self._waiters.append(self.scheduler.current_task)
            yield SyscallWait()
